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

"""
Example usage
# Assumptions

Pub/Sub Subscriptions in the format of PUBSUB_SUBSCRIPTION_TEMPLATE

# ========== config.yaml ==========
date_greg:
  destinationTableIdTemplate: date_greg${run_yyyymmdd}
  destinationTableDescription: 1582-10-15 - 2199-12-31 inclusive
  tableDefs:
    - format: JSON
      maxBadRecords: 0
      encoding: UTF8
      schema:
        fields:
          - fieldName: date
            type: DATE
            description: Date as DATE - CAST('2000-12-31' AS DATE); [1582-10-15, 2199-12-31]

# ========== my_connector.py ==========
class MyConnector(BaseConnector):

    def stage_tables_locally(self, run_ctx: ManagedTransferRun=None, local_prefix=None):
        table_ctx = self.load_date_greg(run_ctx, local_prefix=local_prefix)

        return [table_ctx]

    # NOTE - The argument to table_stager refers to the KEY of the ImportedDataInfo config
    @base_connector.table_stager('date_greg')
    def load_date_greg(self, run_ctx, *args, **kwargs):
        ...
        ...
        ...

        return list_of_uris
# =====================================
"""

import argparse
import copy
import datetime
import functools
import logging
import sys
import tempfile
from typing import List

import google.auth
import path

from google.cloud import pubsub
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer
from googleapiclient import errors

from protobuf_to_dict import protobuf_to_dict
from ruamel.yaml import YAML

from bq_dts import rest_client
from bq_dts import helpers

yaml = YAML(typ='safe')

MAX_TRANSFER_RUN_SECS = 12 * 60.0 * 60.0 # 12 hours
DEFAULT_LOG_FLUSH_SECS = 60                    # 1 minute

# https://cloud.google.com/storage/docs/bucket-locations#available_locations
BQ_DTS_LOCATION_TO_GCS_LOCATION_MAP = {
    'us': {'us'},
    'europe': {'eu'},
    'asia-northeast1': {'asia-northeast1'}
}

##### BEGIN - _Connector  helpers #####
class TableContext(object):
    def __init__(self, imported_data_info=None, table_name=None, uris=None):
        self.imported_data_info = imported_data_info
        self.table_name = table_name
        self.uris = uris

    def to_ImportedDataInfo(self):
        table_idi = copy.deepcopy(self.imported_data_info)
        table_idi.pop('destination_table_id_template', None)
        table_idi['destination_table_id'] = self.table_name
        table_idi['table_defs'][0]['source_uris'] = self.uris
        return table_idi


class TransferRunLogger(logging.Handler):
    LEVEL_TO_SEVERITY_MAP = {
        'INFO': rest_client.MessageSeverity.INFO,
        'WARNING': rest_client.MessageSeverity.WARNING,
        'ERROR': rest_client.MessageSeverity.ERROR
    }

    def __init__(self, level=logging.NOTSET):
        super(TransferRunLogger, self).__init__(level=level)
        self.msgs = list()

    def emit(self, record):
        # NOTE - Output record should be a dict or string depending
        msg_severity = self.LEVEL_TO_SEVERITY_MAP.get(record.levelname)
        if not msg_severity:
            return

        # Convert record to UTC time
        raw_datetime = datetime.datetime.utcfromtimestamp(record.created)
        msg_time = rest_client.to_zulu_time(raw_datetime)
        msg_text = self.format(record)

        out_msg = dict(message_time=msg_time, severity=msg_severity, message_text=msg_text)

        self.msgs.append(out_msg)

    def flush(self):
        self.msgs = list()


class ManagedTransferRun(object):
    """
    Context Manager for BQ DTS Transfer Runs

    On exception - Mark the TransferRun as Failed
    """
    logger_cls = TransferRunLogger

    def __init__(self, transfer_run=None, dts_client=None, logger=None,
                 log_flush_secs=DEFAULT_LOG_FLUSH_SECS, timeout=MAX_TRANSFER_RUN_SECS):
        self.transfer_run = transfer_run
        self.dts_client = dts_client

        # Convenience attributes
        self.name = transfer_run['name']
        self.data_source_id = transfer_run['data_source_id']
        self.project_id, self.location_id, self.config_id, self.run_id = rest_client.parse_transfer_run_name(self.name)
        self.time_start_processing = datetime.datetime.utcnow()

        # Setup logging
        self.logger = logger
        self.run_logger = logger.getChild(f'{self.config_id}.{self.run_id}')
        self.run_logger.setLevel(logging.INFO)

        # And a specific log handler to send TransferMessages to BQ DTS
        self._log_handler = self.logger_cls()
        self._log_handler.setLevel(logging.INFO)
        self.run_logger.addHandler(self._log_handler)

        # Setup timers to...
        # 1 - Flush logs to BQ DTS on a periodic basis
        # 2 - Specify a time-out at the TransferRun level
        self._timer_log_flush_to_bq_dts = helpers.RepeatedTimer(log_flush_secs, self._log_flush)
        self._timer_timeout = helpers.RepeatedTimer(timeout, self._timeout)

    def _log_flush(self):
        """
        Periodically flush self.run_logger to BQ DTS TransferRun.LogMessages

        :return:
        """
        if self.dts_client and self._log_handler.msgs:
            self.dts_client.transfer_run_log_messages(self.name, body=dict(transferMessages=self._log_handler.msgs))

        self._log_handler.flush()

    def _timeout(self):
        # Log an error and raise TimeoutError so self.__exit__ clean-up methods get called
        self.run_logger.error(f'Transfer Run timed out after {self._timer_timeout.interval} second(s)!')
        raise TimeoutError

    def __enter__(self):
        self.logger.info(f'[{self.name}] [STARTING]')

        # Step 1 - Start update BQ DTS and run timeout timers
        self._timer_log_flush_to_bq_dts.start()
        self._timer_timeout.start()

        # Step 2 - If we don't have a BQ DTS client, stop now
        if not self.dts_client:
            return self

        # Step 3 - Explicitly notify BQ DTS that we are starting the run
        self.logger.info(f'[{self.name}] BQ DTS ; Starting Run')

        # Avoid an infinite loop within __enter__ and explicitly call __exit__ on Exception
        try:
            self.dts_client.transfer_run_patch_state(self.name, rest_client.TransferState.RUNNING)
        except Exception:
            if self.__exit__(*sys.exc_info()):
                pass
            else:
                raise

        return self

    def __exit__(self, exc_type, exc, exc_tb):
        # Step 1 - Stop timers
        self._timer_timeout.stop()
        self._timer_log_flush_to_bq_dts.stop()

        # Step 3 - Short-circuit immediately and die if we experience a BQ DTS API exception
        if isinstance(exc, errors.HttpError):
            return False

        # Step 4 - Log the exception to the run
        is_exception = bool(exc_type or exc or exc_tb)

        # Step 5 - Notify BQ DTS of run completion
        if self.dts_client:
            # Step 5a - Update BQ DTS immediately
            self._timer_log_flush_to_bq_dts.run_now()

            # Step 5b - If there's a crash, mark TransferState as FAILED
            if is_exception:
                self.dts_client.transfer_run_patch_state(self.name, rest_client.TransferState.FAILED)
                self.logger.info(f'[{self.name}] BQ DTS ; Finishing Run - FAILED')
            # Step 5c - Otherwise, let DTS know we as a source are done with the run
            else:
                self.dts_client.transfer_run_finish_run(self.name)
                self.logger.info(f'[{self.name}] BQ DTS ; Finishing Run - SUCCESS')

        # Step 6 - Raise an exception IF there is a regular exception, not a DTS API exception
        self.logger.info(f'[{self.name}] [FINISHED]')

        # Step 7 - Suppress exception if AssertionError
        return isinstance(exc, AssertionError)
##### END - _Connector helpers #####


##### BEGIN - _Connector implementation helpers #####
def templatize_table_name(table_template, run_ctx: ManagedTransferRun):
    """

    :param table_template: Python string to format using TransferRun parameters
    :param run_ctx: ManagedTransferRun context

    :return:
    """
    run_time = run_ctx.transfer_run['run_time']

    table_params = copy.deepcopy(run_ctx.transfer_run['params'])
    table_params = table_params or dict()

    table_params['run_time'] = run_time
    table_params['run_yyyymmmdd'] = f'{run_time:%Y%m%d}'
    table_params['user_id'] = run_ctx.transfer_run['user_id']
    return table_template.format(**table_params)

def table_stager(idi_config_name, table_template=None):
    """Convenience decorator - Removes standard boilerplate for table staging functions

     Simplifies table loader function to

     @table_stager(name_of_idi_config)
     def my_function(self, run_ctx, local_prefix):
        return local_uris

    :param idi_config_name:
    :return:
    """
    def instancemethod_wrapper(decorated_fxn):
        @functools.wraps(decorated_fxn)
        def wrapped_fxn(self, run_ctx: ManagedTransferRun, *method_args, **method_kwargs) -> TableContext:
            assert isinstance(self, BaseConnector)

            # Step 1 - Pull ImportedDataInfo from the IDI Configs
            current_idi = self._connector_config['imported_data_info'][idi_config_name]

            # Step 2 - Extract the table name templates
            chosen_table_template = table_template or current_idi['destination_table_id_template']

            # Step 3 - Templatize the table name based on 'params', 'run_date', and 'user_id'
            table_name = templatize_table_name(chosen_table_template, run_ctx)

            # Step 4 - Get the URIs spat out by this function
            uris = decorated_fxn(self, run_ctx, *method_args, **method_kwargs)

            # Step 5 - Create a TableContext and return it
            return TableContext(
                imported_data_info=current_idi,
                table_name=table_name,
                uris=uris
            )

        return wrapped_fxn
    return instancemethod_wrapper
##### END - _Connector implementation helpers #####

class BaseConnector(object):
    ##### BEGIN - Methods to script init options #####
    def __init__(self, credentials=None):
        # Setup GCP Clients
        self._ps_sub_client = None
        self._gcs_client = None
        self._bq_client = None
        self._dts_client = None

        # Setup pre-built RecordSchemas
        self._connector_config = None
        self._required_params_set = None
        self._integer_params_set = None

        default_credentials, self._partner_project_id = google.auth.default()
        self._credentials = credentials or default_credentials

        self.logger = logging.getLogger(self.__class__.__module__)

    def setup_args(self):
        self._parser = argparse.ArgumentParser()

        # Args used for testing/production workloads
        self._parser.add_argument('connector_config', type=path.Path, help='Path to connector.yaml config file')
        self._parser.add_argument('--local-tmpdir', dest='local_tmpdir', type=path.Path, default=tempfile.gettempdir(),
                                  help='Local staging path')
        self._parser.add_argument('--gcs-tmpdir', dest='gcs_tmpdir', type=path.Path, required=True,
                                  help='GCS staging path - "gs://staging-bucket/staging-blob-prefix"')
        self._parser.add_argument('--gcs-overwrite', dest='gcs_overwrite', action='store_true', default=False,
                                  help='Overwrite existing GCS objects if present')

        # Args for controlling background timers
        self._parser.add_argument('--max-transfer-run-secs', dest='max_transfer_run_secs',
                                  type=int, default=MAX_TRANSFER_RUN_SECS,
                                  help='Max seconds we can spend processing a TransferRun before raising a TimeoutError')
        self._parser.add_argument('--log-flush-secs', dest='log_flush_secs', type=int, default=DEFAULT_LOG_FLUSH_SECS,
                                  help='Seconds before flushing logs to BQ DTS')

        # Args used for testing
        self._parser.add_argument('--transfer-run-yaml', dest='transfer_run_yaml', type=path.Path,
                                  help='Path to TransferRun YAML')

        # Args used for production workloads
        self._parser.add_argument('--ps-subname', dest='ps_subname',
                                  help='Subscription name in the format of "bigquerydatatransfer.{data_source_id}.{location_id}.run"')


    def process_args(self, args=None):
        # Step 1 - Parse args
        self._opts = self._parser.parse_args(args=args)

        # Step 2 - Load ImportedDataInfo config file
        connector_config_path = self._opts.connector_config.abspath()
        with connector_config_path.open() as connector_config_fp:
            self._connector_config = yaml.load(connector_config_fp)

        # Step 3 - Data Source Definition parsing
        data_source_dict = self._connector_config['data_source_definition']['data_source']
        # If there are no parameters create one to avoid a null check.
        data_source_dict['parameters'] = data_source_dict['parameters'] or dict()

        self._required_params_set = {
            current_param['param_id'] for current_param in data_source_dict['parameters'] if current_param.get('required')
        }
        self._integer_params_set = {
            current_param['param_id'] for current_param in data_source_dict['parameters'] if current_param['type'] == 'INTEGER'
        }


        self._is_testing = bool(self._opts.transfer_run_yaml)

        # Step 5 - Validate args
        assert self._opts.transfer_run_yaml or self._opts.ps_subname
        assert self._opts.log_flush_secs <= self._opts.max_transfer_run_secs
        # assert self._opts.max_transfer_run_secs <= data_source_dict['update_deadline_seconds']

    ##### END - Methods to script init options #####


    ##### BEGIN - Methods to initiate TransferRun processing #####
    def run(self, args=None):
        self.setup_args()
        self.process_args(args=args)

        if self._is_testing:
            self.trigger_via_file()
        else:
            self.trigger_via_pubsub()

    def trigger_via_file(self):
        self.logger.info(f'Triggering via file - {self._opts.transfer_run_yaml}')

        # Step 2 - Load the a TransferRun from YAML vs via the Pub/Sub subscription
        transfer_run_yaml_path = self._opts.transfer_run_yaml.abspath()
        with transfer_run_yaml_path.open() as fp:
            current_run = yaml.load(fp)

        # Step 3 - Setup a ManagedTransferRun
        with ManagedTransferRun(current_run, dts_client=self.dts_client, logger=self.logger,
                                log_flush_secs=self._opts.log_flush_secs, timeout=self._opts.max_transfer_run_secs) as run_ctx:
            self.process_transfer_run(run_ctx)

    def trigger_via_pubsub(self):
        # Step 1 - Determine the Pub/Sub subscription to subscribe to
        sub_path = self.ps_sub_client.subscription_path(self._partner_project_id, self._opts.ps_subname)
        self.logger.info(f'Triggering via Pub/Sub Subscription => {sub_path}')

        # Step 2 - Setup the Subscription-specific callback, listening for 1 message at a time
        # https://google-cloud-python.readthedocs.io/en/latest/pubsub/subscriber/index.html#pulling-a-subscription
        default_fc = pubsub.types.FlowControl(max_messages=1, max_lease_duration=self._opts.max_transfer_run_secs)
        future = self.ps_sub_client.subscribe(sub_path, callback=self.pubsub_callback, flow_control=default_fc)

        # Step 3 - Block until exception, subscribe uses threads to continue progress
        future.result()


    def pubsub_callback(self, ps_message):
        # Step 1 - Load in a TransferRun message
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#transferrun
        transfer_run_obj = bigquery_datatransfer.types.TransferRun()
        transfer_run_obj.ParseFromString(ps_message.data)

        # Step 2 - Convert to a Python dict
        current_run = protobuf_to_dict(transfer_run_obj, use_enum_labels=True)

        # Step 3 - Setup a ManagedTransferRun
        retry_transfer_run = False
        try:
            with ManagedTransferRun(current_run,
                                    dts_client=self.dts_client, logger=self.logger,
                                    log_flush_secs=self._opts.log_flush_secs,
                                    timeout=self._opts.max_transfer_run_secs) as run_ctx:
                self.process_transfer_run(run_ctx)
        except errors.HttpError as dts_api_error:
            # Step 4a - If there's an unrecoverable BQ DTS API error...
            #   A) Log the error to STDERR
            #   B) Do NOT re-raise the error so we can ack the message off Pub/Sub... otherwise we get into a infinite loop
            if dts_api_error.resp.status in (400, 404):
                self.logger.error(f'Unrecoverable BigQuery Data Transfer Service API error - {dts_api_error.resp.status}')
                self.logger.error(dts_api_error.content.decode("utf-8"))
            # Step 4b - Re-raise the error so we can put this TransferRun back on the Pub/Sub subscription
            else:
                retry_transfer_run = True
        except AssertionError:
            # Step 5 - Do not retry on AssertionErrors, likely caused by invalid parameters
            retry_transfer_run = False

        # Step 6 - Ack the Pub/Sub message
        if retry_transfer_run:
            ps_message.nack()
        else:
            ps_message.ack()

    def validate_transfer_run_params(self, transfer_run_params):
        assert self._required_params_set <= set(transfer_run_params)
        return transfer_run_params

    def process_transfer_run(self, run_ctx):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#transferrun
        # Step 1 - Normalize the RPC-based Transfer Run
        run_ctx.transfer_run = helpers.normalize_transfer_run(run_ctx.transfer_run,
            integer_params=self._integer_params_set)

        # Step 2 - Parse TransferRun Params specific to this Connector
        run_ctx.transfer_run['params'] = self.validate_transfer_run_params(run_ctx.transfer_run['params'])

        # Step 3 - Stage data for your transfer run
        self.logger.info(f'[{run_ctx.name}] [STAGING]')
        gcs_table_ctxs = self.stage_data_for_transfer_run(run_ctx)

        # Step 4 - Kick off load jobs info BigQuery
        if not gcs_table_ctxs:
            self.logger.info(f'[{run_ctx.name}] [LOADING] Nothing to load')
            return

        self.logger.info(f'[{run_ctx.name}] [LOADING]')
        if self._is_testing:
            self.start_bigquery_jobs_via_bq_apis(run_ctx, gcs_table_ctxs)
        else:
            self.start_bigquery_jobs_via_dts_apis(run_ctx, gcs_table_ctxs)
    ##### END - Methods to initiate TransferRun processing #####


    ##### BEGIN - Methods to stage requested data #####
    def stage_data_for_transfer_run(self, run_ctx: ManagedTransferRun):
        """
        Step 1) Stage local tables
        Step 2) Upload local tables to GCS
        Step 3) Setup ImportedDataInfo's for use with startBigQueryJobs

        :param run_ctx:
        :return:
        """
        # Step 1 - Stage local tables @ /tmp/{data_source_id}/{config_id}/{run_id}/
        local_prefix = self._opts.local_tmpdir.joinpath(
            run_ctx.data_source_id, run_ctx.config_id, run_ctx.run_id or 'no_run_id'
        )

        self.logger.info(f'[{run_ctx.name}] Staging local => {local_prefix}')
        local_table_ctxs = self.stage_tables_locally(run_ctx, local_prefix=local_prefix)

        # Step 2 - Use regional GCS bucket and validate this is a valid bucket to stage data in
        gcs_bucket_name, gcs_prefix = helpers.parse_gcs_uri(self._opts.gcs_tmpdir)
        gcs_bucket = self.gcs_client.get_bucket(gcs_bucket_name)

        # Validate that the chosen bucket is co-located with the BigQuery Dataset
        # https://cloud.google.com/storage/docs/bucket-locations
        allowed_gcs_locations = BQ_DTS_LOCATION_TO_GCS_LOCATION_MAP.get(run_ctx.location_id) or set()
        assert gcs_bucket.location.lower() in allowed_gcs_locations

        # Step 3 - Create GCS prefix @ {gcs_tmpdir}/{source}/{config}
        gcs_run_prefix = self._opts.gcs_tmpdir.joinpath(run_ctx.data_source_id, run_ctx.config_id)
        self.logger.info(f'[{run_ctx.name}] Staging GCS path => {gcs_run_prefix}')

        # Step 4 - Upload local tables to GCS
        gcs_table_ctxs = list()
        for current_table_ctx in local_table_ctxs:
            self.logger.info(f'[{run_ctx.name}] Staging GCS table => {current_table_ctx.table_name}')
            # Step 4a - Upload to GCS
            gcs_uris = helpers.upload_multiple_files_to_gcs(self.gcs_client, current_table_ctx.uris,
                local_prefix=local_prefix, gcs_prefix=gcs_run_prefix, overwrite=self._opts.gcs_overwrite)

            # Step 4b - Create TableContexts associating these GCS URIs with their schemas and table names
            out_ctx = TableContext(
                imported_data_info=current_table_ctx.imported_data_info,
                table_name=current_table_ctx.table_name,
                uris=gcs_uris)

            gcs_table_ctxs.append(out_ctx)

        return gcs_table_ctxs

    def stage_tables_locally(self, run_ctx: ManagedTransferRun=None, local_prefix=None) -> List[TableContext]:
        """
        :param run_ctx: ManagedTransferRun
        :param local_prefix: Local directory within which to temporarily stage data

        :return: table_ctxs: List of collections.namedtuple('TableContext', ['table_name', 'tabledef', 'uris'])

        table_name => Substituted from current_run (e.g. from mytable_{params.CustomerID}${run_date})
        tabledef => from self._idi_config[tabledef_name]
        uris => URIs to local files.  Wildcards are not expanded
        """
        raise NotImplementedError
    ##### END - Methods to stage requested data #####


    ##### BEGIN - Methods for loading data into BigQuery #####
    def start_bigquery_jobs_via_dts_apis(self, run_ctx: ManagedTransferRun, gcs_table_ctxs: List[TableContext]):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#google.cloud.bigquery.datatransfer.v1.ImportedDataInfo
        # Step 1 - Prepare the body for "start_big_query_jobs"
        current_run_idis = []
        for current_table_ctx in gcs_table_ctxs:
            self.logger.info(f'[{run_ctx.name}] BQ DTS ; {current_table_ctx.table_name}')
            table_idi = current_table_ctx.to_ImportedDataInfo()
            current_run_idis.append(table_idi)

        body = dict(importedData=current_run_idis)

        # Step 2 - Trigger a single startBigQueryJobs call
        self.logger.info(f'[{run_ctx.name}] BQ DTS ; Starting BigQuery Jobs')
        run_ctx.dts_client.transfer_run_start_big_query_jobs(run_ctx.name, body=body)

    def start_bigquery_jobs_via_bq_apis(self, run_ctx: ManagedTransferRun, gcs_table_ctxs: List[TableContext]):
        """
        Development only method.  Used to validate schema configuration and loading into BigQuery.
        Re-uses and re-interprets table schema as defined in "imported_data_info.yaml"
        :param run_ctx:
        :param gcs_table_ctxs:
        :return:
        """
        # Step 1 - Ensure the target Dataset exists
        dataset_id = run_ctx.transfer_run['destination_dataset_id']

        # Step 2 - Trigger multiple BigQuery Load jobs based on the ImportedDataInfo
        for current_table_ctx in gcs_table_ctxs:
            assert 'sql' not in current_table_ctx.imported_data_info, 'SQL not supported by SDK at this time'

            load_job = helpers.load_bigquery_table_via_bq_apis(self.bq_client,
                dataset_id, current_table_ctx.table_name, current_table_ctx.imported_data_info, current_table_ctx.uris)
            self.logger.info(f'[{run_ctx.name}] BQ Load ; {current_table_ctx.table_name} => {load_job.job_id}')
    ##### END - Methods for self-managed loads #####

    @property
    def ps_sub_client(self):
        if not self._ps_sub_client:
            self._ps_sub_client = pubsub.SubscriberClient(credentials=self._credentials)
        return self._ps_sub_client

    @property
    def gcs_client(self):
        if not self._gcs_client:
            self._gcs_client = storage.Client(credentials=self._credentials)
        return self._gcs_client

    @property
    def bq_client(self):
        if not self._bq_client:
            self._bq_client = bigquery.Client(credentials=self._credentials)
        return self._bq_client

    @property
    def dts_client(self):
        if self._is_testing:
            return None

        if not self._dts_client:
            self._dts_client = rest_client.PartnerDTSClient(credentials=self._credentials)
        return self._dts_client
