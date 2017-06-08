#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" db config class define """

import yaml
import sys
import os

reload(sys)
sys.setdefaultencoding('utf8')

PROJECT_PATH = sys.path[0]

TRACKING_DB_CONF = PROJECT_PATH + '/config/db_tracking.yaml'
METADATA_DB_CONF = PROJECT_PATH + '/config/db_metadata.yaml'
DIFF_SETTING_CONF = PROJECT_PATH + '/config/diff_setting.yaml'
EMAIL_CONF = PROJECT_PATH + '/config/email.yaml'


class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._inst


class ConfigLoader(Singleton):
    tracking_dbs = []
    metadata_db = {}
    diff_setting = {}
    email_conf = {}

    def load_configs(self):
        stream = file(TRACKING_DB_CONF, 'r')
        self.tracking_dbs = yaml.load(stream)
        stream.close()

        stream = file(METADATA_DB_CONF, 'r')
        self.metadata_db = yaml.load(stream)
        stream.close()

        stream = file(DIFF_SETTING_CONF, 'r')
        self.diff_setting = yaml.load(stream)
        stream.close()

        stream = file(EMAIL_CONF, 'r')
        self.email_conf = yaml.load(stream)
        stream.close()

    @staticmethod
    def shareInstance():
        return ConfigLoader()

    @staticmethod
    def diffTableColumns():
        setting = ConfigLoader().diff_setting.get("diff_table", {})
        primary_keys = ConfigLoader().diff_setting.get("table_primary_keys", [])
        return filter(lambda e: setting[e], setting), primary_keys

    @staticmethod
    def diffColumns():
        setting = ConfigLoader().diff_setting.get("diff_column", {})
        primary_keys = ConfigLoader().diff_setting.get("column_primary_keys", [])
        return filter(lambda e: setting[e], setting), primary_keys
