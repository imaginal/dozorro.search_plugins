# -*- coding: utf-8 -*-
import json
import urllib
import urllib2


test_tender = {
    "meta": {
        "id": u"066897f872064c1796176e341683db55",
        "dateModified": u"2017-08-10T12:33:48.424572+03:00"
    },
    "data": {
        "awardCriteria": u"lowestCost",
        "procurementMethod": u"open",
        "submissionMethod": u"electronicAuction",
        "procuringEntity": {
            "contactPoint": {
                "telephone": u"380442720917",
                "faxNumber": u"461-90-47",
                "name": u"Тамара Петрівна",
                "email": u"kist@mail.ua"
            },
            "identifier": {
                "scheme": u"UA-EDR",
                "id": u"22934387",
                "legalName": u"Коледж інформаційних систем"
            },
            "name": u"Коледж інформаційних систем",
            "kind": u"general",
            "address": {
                "postalCode": u"04053",
                "countryName": u"Україна",
                "streetAddress": u"пл. Львівська, 14",
                "region": u"Київська область",
                "locality": u"Київ"
            }
        },
        "owner": u"tender.owner",
        "id": u"066897f872064c1796176e341683db55",
        "description": u"Канцелярські товари",
        "title": u"Канцелярські товари",
        "tenderID": u"UA-2017-08-01-000554-b",
        "items": [
            {
                "description": u"Журнал навчальних заннять - 40 шт.",
                "classification": {
                    "scheme": u"ДК021",
                    "description": u"Канцелярські товари",
                    "id": u"30192700-8"
                },
                "id": u"07dc362a251945c0821ae11a2738963d",
                "unit": {
                    "code": u"H87",
                    "name": u"штуки"
                },
                "quantity": 526
            }
        ],
        "value": {
            "currency": u"UAH",
            "amount": 15000,
            "valueAddedTaxIncluded": True
        }
    }
}


def test_plugin_class():
    from dozorro.search_plugins.tender import SearchPlugin

    assert "dozorro" in SearchPlugin.index_mappings
    assert "match_map" in SearchPlugin.search_maps
    assert "range_map" in SearchPlugin.search_maps


def create_search_plugin():
    from dozorro.search_plugins.tender import SearchPlugin
    from ConfigParser import ConfigParser
    import os

    script_path = os.path.dirname(__file__)

    parser = ConfigParser()
    parser.read(script_path + '/search.ini')

    config = dict(parser.items('search_engine'))

    plugin = SearchPlugin(config)
    return plugin


def drop_test_tables(cursor):
    cursor.execute("DROP TABLE IF EXISTS dozorro_risk_values")
    cursor.execute("DROP TABLE IF EXISTS dozorro_risk_score")
    cursor.execute("DROP TABLE IF EXISTS perevorot_dozorro_json_forms")


def create_test_tables(cursor):
    drop_test_tables(cursor)
    dozorro_risk_values = """
        CREATE TABLE `dozorro_risk_values` (
          `risk_code` varchar(100) NOT NULL DEFAULT '',
          `tender_id` varchar(250) NOT NULL,
          `lot_id` varchar(250) DEFAULT '',
          `risk_value` varchar(100) DEFAULT NULL
        ) DEFAULT CHARSET=utf8;
        """
    cursor.execute(dozorro_risk_values)

    dozorro_risk_score = """
        CREATE TABLE `dozorro_risk_score` (
          `tender_id` varchar(250) NOT NULL,
          `lot_id` varchar(250) DEFAULT NULL,
          `risk_flags` varchar(250) DEFAULT NULL,
          `risk_value` float DEFAULT NULL
        ) DEFAULT CHARSET=utf8;
        """
    cursor.execute(dozorro_risk_score)

    dozorro_json_forms = """
        CREATE TABLE `perevorot_dozorro_json_forms` (
          `object_id` varchar(32) DEFAULT NULL,
          `model` varchar(40) NOT NULL,
          `tender` varchar(255) NOT NULL,
          `schema` varchar(255) NOT NULL,
          `payload` text NOT NULL
        ) DEFAULT CHARSET=utf8;
        """
    cursor.execute(dozorro_json_forms)


def insert_test_data(mysql_config, tender_id, lot_id):
    import MySQLdb
    import warnings
    warnings.filterwarnings("ignore", category=MySQLdb.Warning)

    db = MySQLdb.Connect(**mysql_config)
    cursor = db.cursor()
    create_test_tables(cursor)

    cursor.execute(
        "INSERT INTO dozorro_risk_values (risk_code, tender_id, lot_id, risk_value) " +
        "VALUES (%s, %s, %s, %s)", ('R1020', tender_id, None, 'Yes'))
    cursor.execute(
        "INSERT INTO dozorro_risk_values (risk_code, tender_id, lot_id, risk_value) " +
        "VALUES (%s, %s, %s, %s)", ('R1030', tender_id, lot_id, 2))

    cursor.execute(
        "INSERT INTO dozorro_risk_score (tender_id, lot_id, risk_value) " +
        "VALUES (%s, %s, %s)", (tender_id, None, 0.5))
    cursor.execute(
        "INSERT INTO dozorro_risk_score (tender_id, lot_id, risk_value) " +
        "VALUES (%s, %s, %s)", (tender_id, lot_id, 0.1))

    payload = """{"date":"2017-01-05T19:07:53+02:00","model":"form/tender111","owner":"dozorro.org",
        "payload":{"author":{"auth":{"id":"27900b25700d8882a2b43b9bfe3c556c","provider":"google",
        "scheme":"external"}},"formData":{"argumentativeDisqualification":"no",
        "argumentativeDisqualificationComment":"На мой взгляд, всё очевидно.",
        "cheapestWasDisqualified":"yes","cheapestWasDisqualifiedComment":"Ага, даже две!"},
        "tender":"066897f872064c1796176e341683db55"}}"""

    cursor.execute(
        "INSERT INTO perevorot_dozorro_json_forms (object_id, model, tender, `schema`, payload) " +
        "VALUES (%s, %s, %s, %s, %s)",
        ('a122ce5ecbba80122a7a27d776d568ff', 'form', tender_id, 'F111', payload))

    db.commit()


def test_plugin_mappings():
    plugin = create_search_plugin()

    assert "dozorro" in plugin.index_mappings
    assert "properties" in plugin.index_mappings["dozorro"]
    assert "formModels" in plugin.index_mappings["dozorro"]["properties"]
    assert "formsCount" in plugin.index_mappings["dozorro"]["properties"]
    assert "riskCodes" in plugin.index_mappings["dozorro"]["properties"]
    assert "riskScore" in plugin.index_mappings["dozorro"]["properties"]
    assert "riskValues" in plugin.index_mappings["dozorro"]["properties"]
    assert "forms" in plugin.index_mappings["dozorro"]["properties"]
    assert "date" in plugin.index_mappings["dozorro"]["properties"]["forms"]["properties"]
    assert "id" in plugin.index_mappings["dozorro"]["properties"]["forms"]["properties"]
    assert "model" in plugin.index_mappings["dozorro"]["properties"]["forms"]["properties"]
    assert "owner" in plugin.index_mappings["dozorro"]["properties"]["forms"]["properties"]
    assert "payload" in plugin.index_mappings["dozorro"]["properties"]["forms"]["properties"]


def test_plugin_mysql():
    plugin = create_search_plugin()

    mysql_config = plugin.mysql_config.copy()
    mysql_config['passwd'] = plugin.mysql_passwd
    item = test_tender.copy()
    tender_id = item['meta']['id']
    lot_id = 'aa22ce5ecbba80122a7a27d776d568ff'
    insert_test_data(mysql_config, tender_id, lot_id)

    plugin.cursor = None
    plugin.start_in_subprocess(None)
    assert plugin.cursor

    plugin.cursor = None
    plugin.dbcon.close()
    plugin.before_process_index(None)
    assert plugin.cursor

    item["meta"]["dateModified"] = "2017-08-10T12:33:48.424572+03:00"

    # test dozorro_skip_until
    plugin.before_index_item(None, item)
    assert "dozorro" not in item["data"]

    item["meta"]["dateModified"] = "2018-08-10T12:33:48.424572+03:00"

    item["meta"]["id"] += "x"

    # test miss by tender_id
    plugin.before_index_item(None, item)
    assert "dozorro" not in item["data"]

    item["meta"]["id"] = tender_id

    # finally test dozorro data
    plugin.before_index_item(None, item)
    assert "dozorro" in item["data"]

    assert "R1020" in item["data"]["dozorro"]["riskCodes"]
    assert "R1030" in item["data"]["dozorro"]["riskCodes"]
    assert item["data"]["dozorro"]["riskScore"] == 0.5
    assert item["data"]["dozorro"]["riskValues"] == [{'R1020': 1}, {'lotID': lot_id, 'R1030': 2}]

    assert item["data"]["dozorro"]["formsCount"] == 1
    assert item["data"]["dozorro"]["formModels"] == "F111"
    assert item["data"]["dozorro"]["forms"][0]["date"] == "2017-01-05T19:07:53+02:00"

    drop_test_tables(plugin.cursor)


class SearchHelper(object):
    host = '127.0.0.1'
    port = '8484'

    def __init__(self):
        from ConfigParser import ConfigParser
        import os
        script_path = os.path.dirname(__file__)
        parser = ConfigParser()
        parser.read(script_path + '/search.ini')
        if parser.has_section('server:main'):
            self.host = parser.get('server:main', 'host')
            self.port = parser.get('server:main', 'port')

    def tenders(self, **kw):
        query_string = urllib.urlencode(kw)
        response = urllib2.urlopen("http://{}:{}/tenders?{}".format(self.host, self.port, query_string))
        jsondata = response.read()
        return json.loads(jsondata)


def test_search_tenders():
    search = SearchHelper()

    resp = search.tenders(tid_like="UA-2018-08-01")
    assert resp["total"] > 0

    resp = search.tenders(risk_code="X1001")
    assert resp["total"] == 0

    resp = search.tenders(risk_code="R1020")
    assert resp["total"] > 0

    resp = search.tenders(form_code="X100")
    assert resp["total"] == 0

    resp = search.tenders(form_code="F111")
    assert resp["total"] > 0

    resp = search.tenders(form_count="1000-2000")
    assert resp["total"] == 0

    resp = search.tenders(form_count="1-2")
    assert resp["total"] > 0

    resp = search.tenders(risk_score="10")
    assert resp["total"] == 0

    resp = search.tenders(risk_score="0-1")
    assert resp["total"] > 0

    resp = search.tenders(risk_R1030="1-2")
    assert resp["total"] > 0
