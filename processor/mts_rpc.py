# this module workflow:
# 1.read conf file
# 2.process data with line
# 3.send it to influxDB,then next line


import os
import time

import pendulum
from logbook import Logger

log = Logger('mts_rpc')


def get_timestamp(time_str):
    format_24 = ''
    format_12 = ''
    # 12h time format
    if time_str[-1] == 'M':
        timestamp = pendulum.from_format(time_str, '%m/%d/%Y %I:%M:%S %p',
                                         'Asia/Shanghai').int_timestamp
    else:
        timestamp = pendulum.from_format(time_str, '%Y/%m/%d %H:%M:%S',
                                         'Asia/Shanghai').int_timestamp

    return timestamp


def get_float(f):
    if f == "":
        return 0.0
    else:
        return float(f)


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.front_rear_full_special = conf['processor'][processor]['is_front_rear_full']

        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.seq = 1
        self.tags_dict = [
            'DateTime',  # 0
            'StartSequence',  # 1
            'EventType',  # 2
            'Sequence',  # 3
            'Channel',  # 4
            'LimitValue',  # 5
            'CurrentValue',  # 6
            'Previous',  # 7
            'New',  # 8
            'Unit'  # 9
        ]

    def message_process(self, msgline, task_related, measurement):

        data = msgline.split('\t')
        try:
            timestamp = get_timestamp(data[0])
        except Exception as e:
            log.error('not regular data')
            return 2, None, None
        data_len = len(data)

        switch = {3: self.len3, 7: self.len7, 10: self.len10}

        filename, self.task_position = task_related
        task = self.get_task(filename)

        if self.front_rear_full_special:
            # front , rear, full speical
            source = self.decide_source(filename)
        else:
            # no source in file path.
            source = None

        if data_len in [3, 7, 10]:
            tags, fields = switch[data_len](data, task)
            if source:
                tags['source'] = source

            fields = del_null_fields(fields)

            influx_json = {"tags": tags,
                           "fields": fields,
                           "time": 1000000 * int(timestamp) + self.seq % 1000,
                           "measurement": measurement}

            # seq value.
            if self.seq > 10000:
                self.seq = 0
            self.seq += 1

            return 0, 'process successful', influx_json

        else:
            self.seq += 1
            tags = {"node": self.nodename}

            fields = {"line": msgline, "datetime": time.strftime("%Y-%m-%d %H:%M:%S")}

            # measurement: issueline
            influx_json = {"tags": tags, "fields": fields, "time": int(time.time()), "measurement": "new_issueline_rpc"}
            log.debug("issue_line rpc:")
            return 1, 'wrong format.', influx_json

    def decide_source(self, file_absolute_path):
        task = ['front', 'rear', 'full']
        path_split = file_absolute_path.split(os.sep)

        # 找到相应source关键字
        for one in task:
            if one in path_split:
                return one

        # 没有找到关键字
        source = 'full'
        return source

    def get_task(self, file_absolute_path, ):

        some = self.task_position

        name = file_absolute_path.split(os.sep)[some]
        # if .xxx ,drop it
        if some == -1:
            name = name[:-4]
        task = name
        # else:
        #     task_map = dict([tuple(one.split(':')) for one in some])
        #     path_split = file_absolute_path.split(os.sep)
        #
        #     for one in task_map:
        #         if one in path_split:
        #             task = task_map[one]
        #             return task
        #     task = 'full'
        return task

    def len3(self, data, task):
        tags = {

            "eqpt_no": self.eqpt_no
        }

        fields = {

            "task": task,
            "EventType": data[2],
        }
        return tags, fields

    def len7(self, data, task):
        tags = {

            "eqpt_no": self.eqpt_no
        }

        fields = {
            "Sequence": data[3],
            "Channel": data[4],
            "task": task,
            "EventType": data[2],

            'LimitValue': get_float(data[5]),
            'CurrentValue': get_float(data[6]),
        }
        return tags, fields

    def len10(self, data, task):

        tags = {

            "eqpt_no": self.eqpt_no
        }
        fields = {
            "EventType": data[2],
            "Channel": data[4],

            'LimitValue': get_float(data[5]),
            'CurrentValue': get_float(data[6]),
            'Previous': get_float(data[7]),
            'New': get_float(data[8]),  # 8
            "Sequence": data[3],
            "task": task,
        }
        return tags, fields


def del_null_fields(fields):
    remove_list = []
    for key in fields:
        if fields[key] == 0.0:
            remove_list.append(key)
    if len(remove_list) != 0:
        log.debug('delete fields, {}'.format(remove_list))
    for key in remove_list:
        del fields[key]
    return fields
