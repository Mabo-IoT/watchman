# this module workflow:
# 1.read conf file
# 2.process data with line
# 3.send it to influxDB,then next line


import datetime
import re
import time

from logbook import Logger

log = Logger('mts_rpc')


def get_timestamp(time_str):
    pattern = r'(\d+)/(\d+)/(\d+) (\d+):(\d+):(\d+)'
    compile_obj = re.compile(pattern)
    match_obj = compile_obj.search(time_str)
    if match_obj is None:
        return 0
    all_groups = match_obj.groups()
    month, day, year, hour, minute, second = all_groups

    dp = {"year": int(year), "month": int(month), "day": int(day),
          "hour": int(hour), "minute": int(minute), "second": int(second)}

    dt = datetime.datetime(**dp)

    return time.mktime(dt.timetuple())


def get_float(f):
    if f == "":
        return 0.0
    else:
        return float(f)


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']

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

    def message_process(self, msgline, task, measurement):

        data = msgline.split('\t')
        timestamp = get_timestamp(data[0])

        data_len = len(data)
        switch = {3: self.len3, 7: self.len7, 10: self.len10}
        if data_len in [3, 7, 10]:
            tags, fields = switch[data_len](data, task)

            fields = del_null_fields(fields)
            self.seq += 1

            influx_json = {"tags": tags,
                           "fields": fields,
                           "time": 1000000 * int(timestamp) + self.seq % 1000,
                           "measurement": measurement}

            return influx_json

        else:
            self.seq += 1
            tags = {"node": self.nodename, "task": task, "seq": self.seq}

            fields = {"line": msgline, "datetime": time.strftime("%Y-%m-%d %H:%M:%S")}

            # measurement: issueline
            influx_json = {"tags": tags, "fields": fields, "time": int(time.time()), "measurement": "issueline_rpc"}
            log.debug("issue_line rpc:")
            return influx_json

    def len3(self, data, task):
        tags = {

            "eqpt_no": self.eqpt_no
        }

        fields = {
            "seq": self.seq,
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
            "seq": self.seq,
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
            "seq": self.seq,
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
