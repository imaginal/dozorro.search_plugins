# -*- coding: utf-8 -*-
import MySQLdb
import simplejson as json
from pkgutil import get_data
from logging import getLogger
from openprocurement.search.base_plugin import BasePlugin

logger = getLogger(__name__)


class SearchPlugin(BasePlugin):
    __name__ = __name__

    index_mappings = {
        "dozorro": {
            "include_in_all": False,
            "properties": {
                # see settings/tender.json
            }
        }
    }
    search_maps = {
        'match_map': {
            'risk_code': 'dozorro.riskCodes',
            'form_code': 'dozorro.formModels',
        },
        'range_map': {
            'form_count': 'dozorro.formsCount',
            'risk_score': 'dozorro.riskScore',
            'risk_F1000': 'dozorro.riskValues.F1000',
            'risk_F1100': 'dozorro.riskValues.F1100',
            'risk_F1110': 'dozorro.riskValues.F1110',
            'risk_F1200': 'dozorro.riskValues.F1200',
            'risk_F1210': 'dozorro.riskValues.F1210',
            'risk_F2100': 'dozorro.riskValues.F2100',
            'risk_F2110': 'dozorro.riskValues.F2110',
            'risk_F2120': 'dozorro.riskValues.F2120',
            'risk_F2200': 'dozorro.riskValues.F2200',
            'risk_F2210': 'dozorro.riskValues.F2210',
            'risk_F2220': 'dozorro.riskValues.F2220',
            'risk_R1020': 'dozorro.riskValues.R1020',
            'risk_R1030': 'dozorro.riskValues.R1030',
            'risk_R1040': 'dozorro.riskValues.R1040',
            'risk_R1050': 'dozorro.riskValues.R1050',
            'risk_R1060': 'dozorro.riskValues.R1060',
            'risk_R1070': 'dozorro.riskValues.R1070',
            'risk_R1075': 'dozorro.riskValues.R1075',
            'risk_R1080': 'dozorro.riskValues.R1080',
            'risk_R1090': 'dozorro.riskValues.R1090',
            'risk_R1110': 'dozorro.riskValues.R1110',
            'risk_R1120': 'dozorro.riskValues.R1120',
            'risk_R1130': 'dozorro.riskValues.R1130',
            'risk_R1150': 'dozorro.riskValues.R1150',
            'risk_R1170': 'dozorro.riskValues.R1170',
            'risk_R2000': 'dozorro.riskValues.R2000',
            'risk_R2010': 'dozorro.riskValues.R2010',
            'risk_R2030': 'dozorro.riskValues.R2030',
            'risk_R2040': 'dozorro.riskValues.R2040',
            'risk_R2050': 'dozorro.riskValues.R2050',
            'risk_R3010': 'dozorro.riskValues.R3010',
            'risk_R3020': 'dozorro.riskValues.R3020',
            'risk_R3035': 'dozorro.riskValues.R3035',
            'risk_R3040': 'dozorro.riskValues.R3040',
            'risk_R3050': 'dozorro.riskValues.R3050',
            'risk_R3060': 'dozorro.riskValues.R3060',
            'risk_R3070': 'dozorro.riskValues.R3070',
            'risk_R3080': 'dozorro.riskValues.R3080',
            'risk_R4010': 'dozorro.riskValues.R4010',
            'risk_R4020': 'dozorro.riskValues.R4020',
            'risk_R4071': 'dozorro.riskValues.R4071',
            'risk_R4072': 'dozorro.riskValues.R4072',
            'risk_R4073': 'dozorro.riskValues.R4073',
            'risk_R4074': 'dozorro.riskValues.R4074',
            'risk_R4075': 'dozorro.riskValues.R4075',
            'risk_R4076': 'dozorro.riskValues.R4076',
            'risk_R4077': 'dozorro.riskValues.R4077',
            'risk_R4078': 'dozorro.riskValues.R4078'
        },
        'sorting_map': {
            'risk': 'dozorro.riskScore'
        }
    }
    mysql_config = {
        'host': 'localhost',
        'user': 'dozorro',
        'db': 'dozorro',
        'charset': 'utf8',
        'init_command': 'SET NAMES utf8',
        'connect_timeout': 10,
        'read_timeout': 30,
        'write_timeout': 30
    }
    mysql_vars = {
        'interactive_timeout': 3600,
        'net_read_timeout': 300,
        'net_write_timeout': 300,
        'wait_timeout': 3600
    }
    plugin_config = {
        'risk_values': False,
        'risk_score': False,
        'json_forms': False,
        'skip_until': None
    }

    def __init__(self, config):
        self.index_mappings = json.loads(get_data(__name__, 'settings/tender.json'))
        for key in self.plugin_config.keys():
            dozorro_key = 'dozorro_' + key
            if dozorro_key in config:
                self.plugin_config[key] = config[dozorro_key]
        for key in self.mysql_config.keys():
            dozorro_key = 'dozorro_' + key
            if dozorro_key in config:
                self.mysql_config[key] = config[dozorro_key]
            if '_timeout' in key:
                self.mysql_config[key] = int(self.mysql_config[key])
        self.mysql_passwd = config.get('dozorro_passwd', '').strip(" \t'\"")
        self.create_cursor()

    def create_cursor(self):
        logger.info("Connect to mysql {user}@{host}/{db}".format(**self.mysql_config))
        # close db handle if present
        if getattr(self, 'cursor', None):
            self.cursor = None
        if getattr(self, 'dbcon', None):
            try:
                dbcon, self.dbcon = self.dbcon, None
                dbcon.close()
                del dbcon
            except MySQLdb.MySQLError:
                pass
        self.dbcon = MySQLdb.Connect(passwd=self.mysql_passwd,
            **self.mysql_config)
        self.cursor = self.dbcon.cursor()
        # update session variables
        for k, v in self.mysql_vars.items():
            self.cursor.execute("SET {}={}".format(k, v))

    def start_in_subprocess(self, index):
        self.cursor = None
        self.create_cursor()

    def before_process_index(self, index):
        try:
            self.dbcon.ping(True)
        except MySQLdb.MySQLError as e:
            logger.warning("Error ping mysql %s", str(e))
            self.create_cursor()

    def query_risk_values(self, data, tender_id):
        self.cursor.execute(
            "SELECT risk_code, lot_id, risk_value " +
            "FROM dozorro_risk_values " +
            "WHERE tender_id=%s", (tender_id,))
        risk_dict = {}
        code_dict = {}
        for risk_code, lot_id, risk_value in self.cursor.fetchall():
            if lot_id not in risk_dict:
                risk_dict[lot_id] = {}
            try:
                risk_value = float(risk_value)
            except (TypeError, ValueError):
                risk_value = 1 if risk_value else 0
            risk_dict[lot_id][risk_code] = risk_value
            if risk_code not in code_dict:
                code_dict[risk_code] = 1
        if risk_dict:
            data["riskCodes"] = " ".join(code_dict.keys())
            data["riskValues"] = list()
            for lot_id, risks in risk_dict.items():
                if lot_id:
                    risks["lotID"] = lot_id
                data["riskValues"].append(risks)

    def query_risk_score(self, data, tender_id):
        self.cursor.execute(
            "SELECT lot_id, risk_value " +
            "FROM dozorro_risk_score " +
            "WHERE tender_id=%s AND risk_value > 0", (tender_id,))
        max_value = 0
        for lot_id, risk_value in self.cursor.fetchall():
            if risk_value and risk_value > max_value:
                data["riskScore"] = float(risk_value)
                max_value = risk_value

    def query_json_forms(self, data, tender_id):
        self.cursor.execute(
            "SELECT `object_id`, `schema`, `payload` " +
            "FROM perevorot_dozorro_json_forms " +
            "WHERE tender=%s AND model=%s", (tender_id, 'form'))
        schema_dict = {}
        forms_list = []
        for object_id, schema, payload in self.cursor.fetchall():
            payload = json.loads(payload)
            payload['id'] = object_id
            forms_list.append(payload)
            schema_dict[schema] = 1
        if forms_list:
            data["formModels"] = " ".join(schema_dict.keys())
            data["formsCount"] = len(forms_list)
            data["forms"] = forms_list

    def query_tender_data(self, tender_id):
        data = {}
        if self.plugin_config['risk_values']:
            self.query_risk_values(data, tender_id)
        if self.plugin_config['risk_score']:
            self.query_risk_score(data, tender_id)
        if self.plugin_config['json_forms']:
            self.query_json_forms(data, tender_id)
        return data

    def query_item_data(self, index, item):
        tender_id = item['meta']['id']
        for i in range(3):
            try:
                return self.query_tender_data(tender_id)
            except MySQLdb.MySQLError as e:
                if i > 1:
                    raise
                logger.warning("MySQLError %s", str(e))
                if getattr(index, 'engine', None):
                    index.engine.sleep(10)
                self.create_cursor()

    def before_index_item(self, index, item):
        dateModified = item['meta']['dateModified']
        if self.plugin_config['skip_until'] and dateModified:
            if self.plugin_config['skip_until'] > dateModified:
                return
        dozorro_data = self.query_item_data(index, item)
        if dozorro_data:
            item['data']['dozorro'] = dozorro_data
