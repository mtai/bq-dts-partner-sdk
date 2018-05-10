import argparse
import logging

import google.auth
import path
from ruamel.yaml import YAML

from bq_dts import rest_client

yaml = YAML(typ='safe')

class CommandLinePartnerDTSClient(object):
    ##### BEGIN - Methods to script init options #####
    def __init__(self, credentials=None):
        self._credentials, self._project = google.auth.default()
        self._dts_client = rest_client.PartnerDTSClient(credentials=self._credentials)

        self.logger = logging.getLogger(self.__class__.__module__)

    def setup_args(self):
        self._parser = argparse.ArgumentParser()

        # Args used for testing/production workloads
        self._parser.add_argument('method')
        self._parser.add_argument('--project-id', dest='project_id')
        self._parser.add_argument('--location-id', dest='location_id')
        self._parser.add_argument('--dsd-name', dest='dsd_name')
        self._parser.add_argument('--update-mask', dest='update_mask')
        self._parser.add_argument('--page-token', dest='page_token')

        self._parser.add_argument('--body-yaml', dest='body_yaml', type=path.Path)

    def process_args(self, args=None):
        self._opts = self._parser.parse_args(args=args)
        if not self._opts.body_yaml:
            return

        body_yaml_path = self._opts.body_yaml.abspath()
        assert body_yaml_path.exists()

        with body_yaml_path.open() as body_fp:
            self._body = yaml.load(body_fp)

    def run(self, args=None):
        self.setup_args()
        self.process_args(args=args)

        if self._opts.method == 'create':
            resp = self._dts_client.data_source_definition_create(
                project_id=self._opts.project_id, location_id=self._opts.location_id, body=self._body)
        elif self._opts.method == 'list':
            resp = self._dts_client.data_source_definition_list(
                project_id=self._opts.project_id, location_id=self._opts.location_id, page_token=self._opts.page_token)
        elif self._opts.method == 'get':
            resp = self._dts_client.data_source_definition_get(name=self._opts.dsd_name)
        elif self._opts.method == 'patch':
            resp = self._dts_client.data_source_definition_patch(
                name=self._opts.dsd_name, update_mask=self._opts.update_mask, body=self._body)
        elif self._opts.method == 'enroll':
            body = dict(dataSourceIds=[self._opts.dsd_name])
            resp = self._dts_client.enroll_data_sources(
                project_id=self._opts.project_id, location_id=self._opts.location_id, body=body)

        else:
            raise NotImplementedError

        import pprint
        pprint.pprint(resp)


if __name__ == '__main__':
    CommandLinePartnerDTSClient().run()
