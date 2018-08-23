# -*- coding: utf-8 -*-

from openprocurement.search.base_plugin import BasePlugin


class SearchPlugin(BasePlugin):
    search_maps = {}

    def __init__(self, config):
        pass

    def before_create_index(self, index, index_name, mappings):
        pass

    def before_index_item(self, index, index_name, item):
        pass
