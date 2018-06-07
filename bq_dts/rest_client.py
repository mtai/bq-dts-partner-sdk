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

# https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/
import copy
import datetime
import os
import re

import google.auth
from google.cloud import bigquery
from googleapiclient import discovery


# https://cloud.google.com/bigquery/docs/reference/datatransfer/rest/v1/projects.locations/list
BQ_DTS_LOCATIONS = {'us', 'europe', 'asia-northeast1'}

BQ_DTS_OAUTH_SCOPES = ["https://www.googleapis.com/auth/pubsub", "https://www.googleapis.com/auth/bigquery"]
BQ_DTS_API_ENDPOINT = 'bigquerydatatransfer'
BQ_DTS_API_VERSION = 'v1'

TRANSFER_RUN_NAME_PARSER = re.compile('projects/(.*?)/locations/(.*?)/transferConfigs/(.*?)/runs/(.*?)')
TRANSFER_RUN_NAME_FORMATTER = 'projects/{project_id}/locations/{location_id}/transferConfigs/{config_id}/runs/{run_id}'

DATA_SOURCE_DEFINITION_NAME_PARSER = re.compile('projects/(.*?)/locations/(.*?)/dataSourceDefinitions/(.*?)')
DATA_SOURCE_DEFINITION_NAME_FORMATTER = 'projects/{project_id}/locations/{location_id}/dataSourceDefinitions/{data_source_id}'

BQ_DTS_PARTNER_API_DISCOVERY_DOC_PATH = os.path.join(
    os.path.dirname(__file__), 'api_discovery.json'
)

def parse_transfer_run_name(current_str):
    return TRANSFER_RUN_NAME_PARSER.match(current_str).groups()


def parse_data_source_definition_name(current_str):
    return DATA_SOURCE_DEFINITION_NAME_PARSER.match(current_str).groups()


def from_zulu_time(in_str):
    dt, _, us = in_str.partition(".")
    dt = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
    us = int(us.rstrip("Z"), 10)
    return dt + datetime.timedelta(microseconds=us)


def to_zulu_time(in_datetime):
    return in_datetime.isoformat('T') + 'Z'


def dict_to_camel_case(in_dict):
    out_dict = dict()
    for key, value in in_dict.items():
        if value is None:
            continue

        out_key = to_camel_case(key)
        out_dict[out_key] = value

    return out_dict


def to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


def rpc_duration_to_rest_duration(in_duration):
    return f'{in_duration["seconds"]}s'


class MetaEnum(type):
    def __contains__(cls, item):
        return bool(item is None) or bool(item in cls.__dict__)


class Enum(metaclass=MetaEnum):
    pass


##### BEGIN - TransferRun Helpers #####
class TransferState(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#transferstate
    TRANSFER_STATE_UNSPECIFIED = 'TRANSFER_STATE_UNSPECIFIED'
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    SUCCEEDED = 'SUCCEEDED'
    FAILED = 'FAILED'
    CANCELLED = 'CANCELLED'


class MessageSeverity(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#messageseverity
    MESSAGE_SEVERITY_UNSPECIFIED = 'MESSAGE_SEVERITY_UNSPECIFIED'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'


class Format(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#format
    FORMAT_UNSPECIFIED = 'FORMAT_UNSPECIFIED'
    CSV = 'CSV'
    JSON = 'JSON'
    AVRO = 'AVRO'
    PARQUET = 'PARQUET'
    RECORDIO = 'RECORDIO'
    CAPACITOR = 'CAPACITOR'
    COLUMNIO = 'COLUMNIO'


class Encoding(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#encoding
    ENCODING_UNSPECIFIED = 'FORMAT_UNSPECIFIED'
    UTF8 = 'UTF8'
    ISO_8859_1 = 'ISO_8859_1'


class BQType(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#type_1
    TYPE_UNSPECIFIED = 'TYPE_UNSPECIFIED'
    STRING = 'STRING'
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    RECORD = 'RECORD'
    BYTES = 'BYTES'
    BOOLEAN = 'BOOLEAN'
    TIMESTAMP = 'TIMESTAMP'
    DATE = 'DATE'
    TIME = 'TIME'
    DATETIME = 'DATETIME'
    NUMERIC = 'NUMERIC'


# BQ DTS  - https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#format
# BQ Load - https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load.sourceFormat
BQ_DTS_FORMAT_TO_BQ_SOURCE_FORMAT_MAP = {
    Format.CSV: bigquery.SourceFormat.CSV,
    Format.JSON: bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    Format.AVRO: bigquery.SourceFormat.AVRO,
    Format.PARQUET: bigquery.SourceFormat.PARQUET,
}

BQ_DTS_ENCODING_TO_BQ_ENCODING_MAP = {
    Encoding.UTF8: bigquery.Encoding.UTF_8,
    Encoding.ISO_8859_1: 'ISO-8859-1'
}

def TransferRun(name=None, labels=None, schedule_time=None, run_time=None, error_status=None, start_time=None,
                end_time=None, update_time=None, params=None, destination_dataset_id=None, data_source_id=None,
                state=None, user_id=None, schedule=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#transferrun
    # NOTE - Stripped down notion of TransferRun for patch()
    assert state in TransferState
    return dict_to_camel_case(locals())


def TransferMessage(message_text=None, message_time=None, severity=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#transfermessage
    message_time = message_time or to_zulu_time(datetime.datetime.utcnow())
    severity = severity or MessageSeverity.INFO

    assert message_text
    assert severity in MessageSeverity
    return dict_to_camel_case(locals())


def ImportedDataInfo(sql=None, destination_table_id=None, destination_table_description=None, table_defs=None,
                     user_defined_functions=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#importeddatainfo
    assert destination_table_id
    assert type(table_defs) is list

    idi_rest = dict_to_camel_case(locals())
    if 'tableDefs' in idi_rest:
        idi_rest['tableDefs'] = [TableDefinition(**current_tdef) for current_tdef in idi_rest['tableDefs']]
    return idi_rest


def TableDefinition(table_id=None, source_uris=None, format=None, max_bad_records=None, encoding=None,
                    csv_options=None, schema=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#tabledefinition
    assert type(source_uris) is list
    assert format in Format
    assert encoding in Encoding

    tdef_rest = dict_to_camel_case(locals())
    if 'schema' in tdef_rest:
        tdef_rest['schema'] = RecordSchema(**tdef_rest['schema'])
    if 'csvOptions' in tdef_rest:
        tdef_rest['csvOptions'] = CsvOptions(**tdef_rest['csvOptions'])
    return tdef_rest


def CsvOptions(field_delimiter=None, allow_quoted_newlines=None, quote_char=None, skip_leading_rows=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#csvoptions
    return dict_to_camel_case(locals())


def FieldSchema(field_name=None, type=None, is_repeated=None, description=None, schema=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#fieldschema
    assert type in BQType
    fs_rest = dict_to_camel_case(locals())
    if 'schema' in fs_rest:
        fs_rest['schema'] = RecordSchema(**fs_rest['schema'])
    return fs_rest


def RecordSchema(fields=None):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#recordschema
    rs_rest = dict_to_camel_case(locals())
    if 'fields' in rs_rest:
        rs_rest['fields'] = [FieldSchema(**current_field) for current_field in rs_rest['fields']]
    return rs_rest

##### END - TransferRun Helpers #####


class ParameterType(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#type
    TYPE_UNSPECIFIED = 'TYPE_UNSPECIFIED'
    STRING = 'STRING'
    INTEGER = 'INTEGER'
    DOUBLE = 'DOUBLE'
    BOOLEAN = 'BOOLEAN'
    PLUS_PAGE = 'PLUS_PAGE'


class AuthorizationType(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#authorizationtype
    AUTHORIZATION_TYPE_UNSPECIFIED = 'AUTHORIZATION_TYPE_UNSPECIFIED'
    AUTHORIZATION_CODE = 'AUTHORIZATION_CODE'
    GOOGLE_PLUS_AUTHORIZATION_CODE = 'GOOGLE_PLUS_AUTHORIZATION_CODE'


class DataRefreshType(Enum):
    # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rpc/google.cloud.bigquery.datatransfer.v1#datarefreshtype
    DATA_REFRESH_TYPE_UNSPECIFIED = 'DATA_REFRESH_TYPE_UNSPECIFIED'
    SLIDING_WINDOW = 'SLIDING_WINDOW'
    CUSTOM_SLIDING_WINDOW = 'CUSTOM_SLIDING_WINDOW'


##### BEGIN - DataSource Helpers #####
def DataSourceDefinition(name=None, data_source=None, transfer_run_pubsub_topic=None,
                         run_time_offset=None, support_email=None, disabled=None, supported_location_ids=None):
    dsd_rest = dict_to_camel_case(locals())
    if 'dataSource' in dsd_rest:
        dsd_rest['dataSource'] = DataSource(**dsd_rest['dataSource'])
    if 'runTimeOffset' in dsd_rest:
        dsd_rest['runTimeOffset'] = rpc_duration_to_rest_duration(run_time_offset)
    return dsd_rest


def DataSource(name=None, data_source_id=None, display_name=None, description=None, client_id=None, scopes=None,
               update_deadline_seconds=None, default_schedule=None, supports_custom_schedule=None, parameters=None,
               help_url=None, authorization_type=None, data_refresh_type=None, default_data_refresh_window_days=None,
               manual_runs_disabled=None, minimum_schedule_interval=None, partner_legal_name=None):

    assert authorization_type in AuthorizationType
    assert data_refresh_type in DataRefreshType

    ds_rest = dict_to_camel_case(locals())
    if 'parameters' in ds_rest:
        ds_rest['parameters'] = [
            DataSourceParameter(**current_param) for current_param in ds_rest['parameters']
        ]
    if 'minimumScheduleInterval' in ds_rest:
        ds_rest['minimumScheduleInterval'] = rpc_duration_to_rest_duration(minimum_schedule_interval)
    return ds_rest


def DataSourceParameter(param_id=None, display_name=None, description=None, type=None, required=None,
                        validation_regex=None, allowed_values=None, min_value=None, max_value=None,
                        validation_description=None, validation_help_url=None, immutable=None):
    assert type in ParameterType
    param_rest = dict_to_camel_case(locals())
    return param_rest
##### END - DataSource Helpers #####

class PartnerDTSClient(object):
    def __init__(self, credentials=None):
        # project_id = Customer's Project ID
        if credentials is None:
            credentials, project_id = google.auth.default(scopes=BQ_DTS_OAUTH_SCOPES)

        with open(BQ_DTS_PARTNER_API_DISCOVERY_DOC_PATH) as fp:
            bq_dts_discover_doc = fp.read()

        self._rest_client = discovery.build_from_document(service=bq_dts_discover_doc, credentials=credentials)

    def _transfer_run_api_call(self, method_name, **kwargs):
        """
        Convenience method

        > return self._api_call('finishRun')

        Equivalent to

        > return self._rest_client.projects().locations().transferConfigs().runs().finishRun(
            projectId=self.project_id,
            locationId=self.location_id,
            config_id=self.config_id,
            run_id=self.run_id
        ).execute()
        """
        api_prefix_fxn = self._rest_client.projects().locations().transferConfigs().runs()
        api_fxn = getattr(api_prefix_fxn, method_name)
        api_request = api_fxn(**kwargs)

        return api_request.execute()

    def enroll_data_sources(self, project_id=None, location_id=None, body=None):
        base_api_fxn = self._rest_client.projects().locations().enrollDataSources(
            name=f'projects/{project_id}/locations/{location_id}',
            body=body
        )
        return base_api_fxn.execute()

    def get_credentials(self, location_id=None, data_source_id=None):
        base_api_fxn = self._rest_client.projects().locations().dataSources().credentials(
            project_id='-',
            location_id=location_id,
            data_source_id=data_source_id,
        )
        return base_api_fxn.execute()

    def transfer_run_finish_run(self, transfer_run_name):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/finishRun
        return self._transfer_run_api_call('finishRun', name=transfer_run_name, body=dict())

    def transfer_run_log_messages(self, transfer_run_name, body):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/logMessages
        lg_body = copy.deepcopy(body)
        lg_body['transferMessages'] = [TransferMessage(**current_msg) for current_msg in body['transferMessages']]
        return self._transfer_run_api_call('logMessages', name=transfer_run_name, body=lg_body)

    def transfer_run_patch(self, transfer_run_name, body, update_mask=''):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/patch
        tr_body = TransferRun(**body)
        return self._transfer_run_api_call('patch', name=transfer_run_name, updateMask=update_mask, body=tr_body)

    def transfer_run_patch_state(self, transfer_run_name, state):
        return self.transfer_run_patch(transfer_run_name, body=dict(state=state), update_mask='state')

    def transfer_run_start_big_query_jobs(self, transfer_run_name, body):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.transferConfigs.runs/startBigQueryJobs

        start_jobs_body = copy.deepcopy(body)
        start_jobs_body['importedData'] = [ImportedDataInfo(**current_idi) for current_idi in start_jobs_body['importedData']]
        return self._transfer_run_api_call('startBigQueryJobs', name=transfer_run_name, body=start_jobs_body)

    def _data_source_definition_api_call(self, method_name, **kwargs):
        base_api_fxn = self._rest_client.projects().locations().dataSourceDefinitions()
        api_fxn = getattr(base_api_fxn, method_name)

        api_request = api_fxn(**kwargs)
        return api_request.execute()

    def data_source_definition_create(self, project_id=None, location_id=None, body=None):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.dataSourceDefinitions/create
        assert body is not None

        parent = self.data_source_definition_path(project_id=project_id, location_id=location_id)
        dsd_body = DataSourceDefinition(**body)

        return self._data_source_definition_api_call('create', parent=parent, body=dsd_body)

    def data_source_definition_list(self, project_id=None, location_id=None, page_token=None):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.dataSourceDefinitions/list

        parent = self.data_source_definition_path(project_id=project_id, location_id=location_id)
        return self._data_source_definition_api_call('list', parent=parent, pageToken=page_token)

    def data_source_definition_get(self, name=None):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.dataSourceDefinitions/get
        return self._data_source_definition_api_call('get', name=name)

    def data_source_definition_patch(self, name=None, update_mask=None, body=None):
        # https://cloud.google.com/bigquery/docs/reference/data-transfer/partner/rest/v1/projects.locations.dataSourceDefinitions/patch
        dsd_body = DataSourceDefinition(**body)
        return self._data_source_definition_api_call('patch', name=name, updateMask=update_mask, body=dsd_body)

    @classmethod
    def data_source_definition_path(cls, project_id=None, location_id=None, data_source_id=None):
        if data_source_id:
            return f'projects/{project_id}/locations/{location_id}/dataSourceDefinitions/{data_source_id}'

        return f'projects/{project_id}/locations/{location_id}'
