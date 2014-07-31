#!/usr/local/bin/python

import sys, time, subprocess
import boto.dynamodb2.table, boto.dynamodb2.fields, boto.dynamodb2.types
import plistlib, json
import statd

class Batteryd():
    def __init__(self, pidfile, table):
        self._stat = statd.Statd(pidfile)

        def get_data():
            battery_output = subprocess.check_output('system_profiler -xml SPPowerDataType', shell=True)
            battery_output = plistlib.readPlistFromString(battery_output)
            battery_output = battery_output[0]
            return statd.Statd.clean_for_json_parse(battery_output)

        self._stat.set_table(table)
        self._stat.set_get_data(get_data)

    def start(self):
        self._stat.start() 
    def stop(self):
        self._stat.stop()
    def restart(self):
        self._stat.restart()

if __name__ == "__main__":
    table = boto.dynamodb2.table.Table('dsltn_battery', connection=boto.dynamodb2.connect_to_region('us-west-2'))
    batteryd = Batteryd('/tmp/batteryd.pid', table)
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            batteryd.start()
        elif 'stop' == sys.argv[1]:
            batteryd.stop()
        elif 'restart' == sys.argv[1]:
            batteryd.restart()
        elif 'analyze' == sys.argv[1]:
            if raw_input('[G]raphics, [C]ount: ') == 'C':
                table = boto.dynamodb2.table.Table('dsltn_battery', connection=boto.dynamodb2.connect_to_region('us-west-2'))
            start_time = raw_input('Input start time: ')
            table = boto.dynamodb2.table.Table('dsltn_battery', connection=boto.dynamodb2.connect_to_region('us-west-2'))

            events = table.scan(_timeStamp__gte=start_time)
            for event in events:
                print event['Event ID']
        elif 'reset' == sys.argv[1]:
            confirm = raw_input('Are you sure you want to reset? [Y/n]')
            if confirm != 'Y':
                print 'No reset performed.'
                sys.exit(1)
            table = boto.dynamodb2.table.Table('dsltn_battery', connection=boto.dynamodb2.connect_to_region('us-west-2'))
            assert(table.delete())
            table = boto.dynamodb2.table.Table.create('dsltn_battery', schema=[
                boto.dynamodb2.fields.HashKey('Event ID', data_type=boto.dynamodb2.types.NUMBER)
            ], throughput={
                'read': 3,
                'write': 5,
            }, global_indexes={
                boto.dynamodb2.fields.GlobalAllIndex('CreatedIndex', parts=[
                    boto.dynamodb2.fields.HashKey('_timeStamp'),
                ],
                throughput={
                    'read': 4,
                    'write': 2,
                }),
            }, connection=boto.dynamodb2.connect_to_region('us-west-2')
            )
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart|analyze|reset" % sys.argv[0]
        sys.exit(2)
