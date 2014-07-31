#!/usr/local/bin/python

import boto.dynamodb2, boto.dynamodb2.table
import json, sys, time
import daemon

class NotImplementedError(Exception):
    pass

class Statd(daemon.Daemon):
    @staticmethod
    def clean_for_json_parse(dictionary):
        for key in dictionary:
            if type(dictionary[key]) not in [int, float, str, unicode, list, dict]:
                dictionary[key] = str(dictionary[key])
        return dictionary

    @staticmethod
    def _clean_json(json_data):
        for key in json_data:
            if type(json_data[key]) not in [int, float, str, unicode]:
                json_data[key] = json.dumps(json_data[key])
        return json_data

    def _get_data():
        raise NotImplementedError

    def set_table(self, table):
        self._table = table

    def set_get_data(self, get_data):
        self._get_data = get_data

    def run(self):
        self._elements = self._table.count()
        while True:
            output = self._get_data()

            item_data = json.loads(json.dumps(output))
            item_data['Event ID'] = self._elements

            item_data = self._clean_json(item_data)

            self._table.put_item(data=item_data, overwrite=True)

            self._elements = self._elements + 1
            time.sleep(60)
