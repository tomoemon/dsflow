# -*- coding: utf-8 -*-
from google.cloud.proto.datastore.v1 import entity_pb2
from google.cloud.proto.datastore.v1 import query_pb2
from googledatastore import helper as ds_helper
from googledatastore import PropertyFilter


OPERATORS = {
    '<=': query_pb2.PropertyFilter.LESS_THAN_OR_EQUAL,
    '>=': query_pb2.PropertyFilter.GREATER_THAN_OR_EQUAL,
    '<': query_pb2.PropertyFilter.LESS_THAN,
    '>': query_pb2.PropertyFilter.GREATER_THAN,
    '=': query_pb2.PropertyFilter.EQUAL,
}


class Key(object):
    def __init__(self, kind, key):
        self._key = entity_pb2.Key()
        self.add(kind, key)

    def add(self, kind, key):
        ds_helper.add_key_path(self._key, kind, key)

    @property
    def pb(self):
        return self._key


class Query(object):
    def __init__(self, kind=None):
        self._query = query_pb2.Query()
        if kind:
            ds_helper.set_kind(self._query, kind)

    @property
    def pb(self):
        return self._query

    def keys_only(self):
        ds_helper.add_projection(self._query, "__key__")

    def add_filter(self, property_name, operator, value):
        op = OPERATORS.get(operator)
        if op is None:
            error_message = 'Invalid expression: "%s"' % (operator,)
            choices_message = 'Please use one of: =, <, <=, >, >=.'
            raise ValueError(error_message, choices_message)

        ds_helper.set_property_filter(self._query.filter, property_name, op, value)
