# Copyright 2018 Google LLC All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import datetime
import gzip
import math
import json
import re
import threading
import time
from typing import Dict

from google.cloud import bigquery
from google.cloud import exceptions
from google.cloud.bigquery import LoadJobConfig


class RepeatedTimer(object):
    def __init__(self, interval, fxn, *args, **kwargs):
        self.interval = interval
        self.is_running = False

        self._fxn = fxn
        self._args = args
        self._kwargs = kwargs

        self._timer = None
        self._next_time = None

    def _run(self):
        self.is_running = False
        self.start()
        self.run_now()

    def run_now(self):
        self._fxn(*self._args, **self._kwargs)

    def start(self):
        if self.is_running:
            return

        if self._next_time is None:
            self._next_time = time.time()

        self._next_time += self.interval
        self._timer = threading.Timer(self._next_time - time.time(), self._run)
        self._timer.start()
        self.is_running = True

    def stop(self):
        if self._timer:
            self._timer.cancel()

        self.is_running = False


############################# BEGIN - JSON Helpers ############################
NEWLINE = '\n'
_json_dump = json.dump

class GzippedJSONWriter(object):
    """
    Utility class (not used by BQLoader) to write NEWLINE_DELIMITED_JSON

    Example usage

    with GzippedJSONWriter(filename) as json_writer:
        json_writer.write(python_dict_1)
        json_writer.write(python_dict_2)
        json_writer.write(python_dict_3)
        json_writer.write(python_dict_4)
    """
    def __init__(self, filename):
        self._fp = gzip.open(filename, 'wt')

    def write(self, data: Dict):
        _json_dump(data, self._fp)
        self._fp.write(NEWLINE)

    def read(self, bytes=None):
        raise NotImplementedError

    def __enter__(self):
        self._fp.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._fp.__exit__(exc_type, exc_val, exc_tb)
############################# END - JSON Helpers ############################


##### BEGIN - GCS Helpers #####
GCS_URI_PARSER = re.compile('gs://(.*?)/(.*?)$')
def parse_gcs_uri(current_str):
    return GCS_URI_PARSER.match(current_str).groups()

def upload_multiple_files_to_gcs(gcs_client, local_uris, local_prefix=None, gcs_prefix=None, overwrite=False):
    gcs_bucket_cache = dict()
    output_gcs_uris = list()

    # For each Local URI
    for current_uri in local_uris:
        assert current_uri.startswith(local_prefix)

        # Step 1 - Convert local file path to GCS file path
        gcs_uri = current_uri.replace(local_prefix, gcs_prefix)

        # Step 2 - Parse out the GCS Bucket and Blob separately
        gcs_bucket, gcs_blob = parse_gcs_uri(gcs_uri)

        # Step 3 - Fetch the target GCS bucket
        bucket_obj = gcs_bucket_cache.get(gcs_bucket)
        if not bucket_obj:
            bucket_obj = gcs_client.get_bucket(gcs_bucket)
            gcs_bucket_cache[gcs_bucket] = bucket_obj

        # Step 4 - Check if the GCS Blob exists
        blob_obj = bucket_obj.blob(gcs_blob)

        if not blob_obj.exists() or overwrite:
            # Step 5 - Upload the file
            blob_obj.upload_from_filename(filename=current_uri)

        # Step 6 - Keep track of the new GCS URIs
        output_gcs_uris.append(gcs_uri)

    return output_gcs_uris
##### END - GCS Helpers #####


##### BEGIN - BQ DTS API helpers #####
TRANSFER_RUN_TIMESTAMP_FIELDS = ['run_time', 'schedule_time', 'update_time']


def normalize_transfer_run(transfer_run, integer_params=None):
    """
    1) Converts TransferRun.params - Protobuf Structs to Python dict
    2) Casts TransferRun.params integers to ints
    3) Converstions TransferRun.{timestamp_fields} from RPCTimestamp to Python Datetime

    :param transfer_run:
    :param integer_params:
    :return:
    """
    integer_params = integer_params or list()
    out_transfer_run = copy.deepcopy(transfer_run)

    # Step 1 - Convert a Protobuf struct to a Python dict
    # FROM
    #   param_name: {string_value: '1901-01-01'}
    # TO
    #   param_name: '1901-01-01'
    out_params = protobuf_struct_to_python_dict(out_transfer_run['params'])

    # Step 2 - For BQ DTS specifically, cast explicit params to integers
    #
    for int_param in integer_params:
        out_params[int_param] = int(out_params[int_param])

    out_transfer_run['params'] = out_params

    # Step 3 - Convert Proto timestamp to Python Datetime
    for timestamp_param in TRANSFER_RUN_TIMESTAMP_FIELDS:
        out_transfer_run[timestamp_param] = rpc_timestamp_to_datetime(out_transfer_run[timestamp_param])

    return out_transfer_run


def protobuf_struct_to_python_dict(raw_struct):
    # https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#struct
    output_dict = dict()
    for var_name, var_value in raw_struct['fields'].items():
        output_dict[var_name] = _struct_value_to_python_value(var_value)

    return output_dict


def _struct_value_to_python_value(struct_val):
    # https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#value
    assert len(struct_val) == 1
    field_type, field_value = list(struct_val.items())[0]
    field_fxn = VALUE_TO_PYTHON_VALUE_MAP[field_type]
    python_value = field_fxn(field_value)
    return python_value

VALUE_TO_PYTHON_VALUE_MAP = {
    'null_value': lambda value: None,
    'number_value': float,
    'string_value': str,
    'bool_value': lambda value: bool(value == 'true'),
    'struct_value': _struct_value_to_python_value,
    'list_value': lambda value: [_struct_value_to_python_value(current_item) for current_item in value]
}

NANOSECONDS = math.pow(10, -9)
def rpc_timestamp_to_datetime(in_timestamp):
    second_from_epoch = float(in_timestamp['seconds'])
    second_from_epoch += float(in_timestamp.get('nanos', 0.0) * NANOSECONDS)
    return datetime.datetime.utcfromtimestamp(second_from_epoch)
##### END - BQ DTS API helpers #####

##### BEGIN - BQ DTS and BigQuery Helpers #####
BQ_JOB_ID_MATCHER = re.compile('[^a-zA-Z0-9_-]')


def RPCFieldSchema_to_GCloudSchemaField(field_schema):
    """
    https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#fieldschema
    TO
    https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html#tables
    bigquery.SchemaField

    :param field_schema:
    :return:
    """
    bq_schema_field = dict()
    bq_schema_field['name'] = field_schema['field_name']
    bq_schema_field['field_type'] = field_schema['type']
    bq_schema_field['description'] = field_schema['description']
    bq_schema_field['fields'] = RPCRecordSchema_to_GCloudSchema(field_schema.get('schema'))
    return bigquery.SchemaField(**bq_schema_field)


def RPCRecordSchema_to_GCloudSchema(record_schema):
    """
    https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#recordschema
    TO

    https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/usage.html#tables
    list of bigquery.SchemaField

    :param record_schema:
    :return:
    """
    if not record_schema:
        return list()

    return [RPCFieldSchema_to_GCloudSchemaField(current_field) for current_field in record_schema['fields']]


def DTSTableDefinition_to_BQLoadJobConfig(dts_tabledef):
    """
    https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#tabledefinition

    TO

    https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/reference.html#google.cloud.bigquery.job.LoadJob

    :param dts_tabledef:
    :return:
    """
    from bq_dts import rest_client
    job_config = LoadJobConfig()

    dts_schema = RPCRecordSchema_to_GCloudSchema(dts_tabledef['schema'])
    job_config.schema = dts_schema

    # BQ DTS does not provide controls for the following dispositions
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    if 'format' in dts_tabledef:
        dts_format = dts_tabledef['format']
        source_format = rest_client.BQ_DTS_FORMAT_TO_BQ_SOURCE_FORMAT_MAP[dts_format]
        assert source_format is not None
        job_config.source_format = source_format

    if 'max_bad_records' in dts_tabledef:
        job_config.max_bad_records = dts_tabledef['max_bad_records']

    if 'encoding' in dts_tabledef:
        dts_encoding = dts_tabledef['encoding']
        job_config.encoding = rest_client.BQ_DTS_ENCODING_TO_BQ_ENCODING_MAP[dts_encoding]

    if 'csv_options' in dts_tabledef:
        csv_opts = dts_tabledef['csv_options']
        if 'field_delimiter' in csv_opts:
            job_config.field_delimiter = csv_opts['field_delimiter']
        if 'allow_quoted_newlines' in csv_opts:
            job_config.allow_quoted_newlines = csv_opts['allow_quoted_newlines']
        if 'quote_char' in csv_opts:
            job_config.quote_character = csv_opts['quote_char']
        if 'skip_leading_rows' in csv_opts:
            job_config.skip_leading_rows = csv_opts['skip_leading_rows']

    return job_config


def load_bigquery_table_via_bq_apis(bq_client: bigquery.Client, dataset_id, table_name, imported_data_info, src_uris):
    """
    Load tables using BigQuery Load jobs, using the same configuration as BQ DTS ImportedDataInfo
    :return:
    """
    # https://googlecloudplatform.github.io/google-cloud-python/latest/_modules/google/cloud/bigquery/client.html#Client.load_table_from_uri
    # Step 1 - Translate required fields for BigQuery Python SDK
    tgt_tabledef = imported_data_info['table_defs'][0]

    # Step 2 - Create target table if it doesn't exist
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    try:
        bq_client.get_table(table_ref)
    except exceptions.NotFound:
        # Step 2a - Attach schema
        tgt_schema = RPCRecordSchema_to_GCloudSchema(tgt_tabledef['schema'])
        tgt_table = bigquery.Table(table_ref, schema=tgt_schema)

        # Step 2b - Attach description
        tgt_table.description = imported_data_info['destination_table_description']

        # Step 2c - Conditionally set partitioning type
        if '$' in table_name:
            tgt_table.partitioning_type = 'DAY'
            tgt_table._properties['tableReference']['tableId'], _, _ = table_name.partition('$')

        # Step 2d - Create BigQuery table
        bq_client.create_table(tgt_table)

    # Step 3a - Create BigQuery Load Job ID
    current_datetime = datetime.datetime.utcnow().isoformat()
    raw_job_id = f'{table_name}_{current_datetime}'
    clean_job_id = BQ_JOB_ID_MATCHER.sub('___', raw_job_id)

    # Step 3b - Create BigQuery Job Config
    job_config = DTSTableDefinition_to_BQLoadJobConfig(tgt_tabledef)

    # Step 4 - Execute BigQuery Load Job using Python SDK
    load_job = bq_client.load_table_from_uri(source_uris=src_uris, destination=table_ref,
                                             job_id=clean_job_id, job_config=job_config)

    return load_job
##### END - BQ DTS and BigQuery Helpers #####
