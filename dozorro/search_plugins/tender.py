# -*- coding: utf-8 -*-
import MySQLdb
import simplejson as json
from time import mktime
from datetime import datetime
from iso8601 import parse_date
from pkgutil import get_data
from logging import getLogger
from munch import munchify

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
        'fulltext_map': {
            'risk_code_all': 'dozorro.riskCodes',
            'form_code_all': 'dozorro.formModels',
        },
        'match_map': {
            'risk_code': 'dozorro.riskCodes',
            'form_code': 'dozorro.formModels',
        },
        'range_map': {
            'form_count': 'dozorro.formsCount',
            'risk_score': 'dozorro.riskScore',
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
        'skip_after': None,
        'skip_until': None,
        'load_list': False,
        'list_limit': 1000,
    }
    tenders_list = []
    reset_counter = 0
    stat_skipped = 0
    stat_changed = 0
    stat_version = 0


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

    def before_source_reset(self, index):
        self.tenders_list = []
        self.reset_counter = 1000
        if not self.plugin_config['load_list']:
            return

        logger.info("Dozorro plugin reset source list...")

        if self.plugin_config['risk_values']:
            self.cursor.execute(
                "SELECT tender_id, MAX(date) " +
                "FROM dozorro_risk_values " +
                "WHERE risk_code LIKE 'R%' " +
                "GROUP BY tender_id")
        for tender_id, risk_date in self.cursor.fetchall():
            self.tenders_list.append(dict(id=tender_id, dateModified=risk_date))

        if self.plugin_config['json_forms']:
            self.cursor.execute(
                "SELECT tender, MAX(date) " +
                "FROM perevorot_dozorro_json_forms " +
                "WHERE model = 'form' " +
                "GROUP BY tender_id")
        for tender_id, form_date in self.cursor.fetchall():
            self.tenders_list.append(dict(id=tender_id, dateModified=form_date))

        logger.info("Dozorro plugin loaded list of %d tenders", len(self.tenders_list))

    def before_source_items(self, index):
        if not self.tenders_list:
            self.reset_counter -= 1
            if self.reset_counter < 0:
                self.source_reset()
            if not self.tenders_list:
                return

        limit = int(self.plugin_config['list_limit'])
        if limit > len(self.tenders_list):
            limit = len(self.tenders_list)

        logger.info("Dozorro plugin preload %d of %s tenders", limit, len(self.tenders_list))
        for item in self.tenders_list[:limit]:
            if isinstance(item['dateModified'], datetime):
                item['dateModified'] = item['dateModified'].isoformat()
            item['doc_type'] = index.source.__doc_type__
            item['version'] = self.get_version(item['dateModified'])
            item['ignore_exists'] = True
            yield munchify(item)

        self.tenders_list = self.tenders_list[limit:]

    def query_risk_values(self, data, tender_id):
        self.cursor.execute(
            "SELECT risk_code, lot_id, risk_value, date " +
            "FROM dozorro_risk_values " +
            "WHERE tender_id=%s AND risk_code LIKE %s", (tender_id, 'R%'))
        risk_dict = {}
        code_dict = {}
        max_date = ''
        for risk_code, lot_id, risk_value, risk_date in self.cursor.fetchall():
            if risk_code[0] != 'R' or risk_value is None:
                continue
            if lot_id not in risk_dict:
                risk_dict[lot_id] = {}
            try:
                risk_value = float(risk_value)
            except (TypeError, ValueError):
                risk_value = 1 if risk_value else 0
            risk_dict[lot_id][risk_code] = risk_value
            if isinstance(risk_date, datetime):
                risk_date = risk_date.isoformat()
            if risk_date > max_date:
                max_date = risk_date
            if risk_code not in code_dict:
                code_dict[risk_code] = 1
        if risk_dict:
            if max_date > data.get("dateModified", ""):
                data["dateModified"] = max_date
            data["riskCodes"] = " ".join(sorted(code_dict.keys()))
            data["riskValues"] = list()
            for lot_id, risks in risk_dict.items():
                if lot_id:
                    risks["lotID"] = lot_id
                data["riskValues"].append(risks)

    def query_risk_score(self, data, tender_id):
        self.cursor.execute(
            "SELECT lot_id, MAX(risk_value) " +
            "FROM dozorro_risk_score " +
            "WHERE tender_id=%s AND risk_value > 0", (tender_id,))
        max_value = 0
        for lot_id, risk_value in self.cursor.fetchall():
            if risk_value and risk_value > max_value:
                data["riskScore"] = float(risk_value)
                max_value = risk_value

    def query_json_forms(self, data, tender_id):
        self.cursor.execute(
            "SELECT `object_id`, `date`, `schema`, `payload` " +
            "FROM perevorot_dozorro_json_forms " +
            "WHERE tender=%s AND model=%s", (tender_id, 'form'))
        schema_dict = {}
        forms_list = []
        max_date = ''
        for object_id, form_date, schema, payload in self.cursor.fetchall():
            payload = json.loads(payload)
            payload['id'] = object_id
            forms_list.append(payload)
            if isinstance(form_date, datetime):
                form_date = form_date.isoformat()
            if form_date > max_date:
                max_date = form_date
            if schema not in schema_dict:
                schema_dict[schema] = 1
        if forms_list:
            if max_date > data.get("dateModified", ""):
                data["dateModified"] = max_date
            data["formModels"] = " ".join(sorted(schema_dict.keys()))
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

    def get_version(self, dateModified):
        if len(dateModified) < 18 or dateModified[:3] != "201":  # TODO fix in 2020
            return 0
        dt = parse_date(dateModified.replace(" ", "T"))
        version = 1e6 * mktime(dt.timetuple()) + dt.microsecond
        return long(version)

    def before_index_item(self, index, item):
        dateModified = item['meta']['dateModified']
        if self.plugin_config['skip_until'] and dateModified:
            if self.plugin_config['skip_until'] > dateModified:
                self.stat_skipped += 1
                return
        if self.plugin_config['skip_after'] and dateModified:
            if self.plugin_config['skip_after'] < dateModified:
                self.stat_skipped += 1
                return

        dozorro_data = self.query_item_data(index, item)
        if not dozorro_data:
            return

        if 'dateModified' in dozorro_data:
            if isinstance(dozorro_data['dateModified'], datetime):
                dozorro_data['dateModified'] = dozorro_data['dateModified'].isoformat()
            version = self.get_version(dozorro_data['dateModified'])
            if version > item['meta']['version']:
                item['meta']['version'] = version
                self.stat_version += 1

        item['data']['dozorro'] = dozorro_data
        self.stat_changed += 1

        if self.stat_changed % 1000 == 0:
            index_name = index.next_index_name if index.next_index_name else index.current_index
            logger.info("[%s] Dozorro plugin %s skipped %d meta and %d data changed",
                index_name, self.stat_skipped, self.stat_version, self.stat_changed)
