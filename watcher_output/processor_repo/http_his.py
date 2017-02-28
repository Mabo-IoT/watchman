# this module workflow:
# 1.read conf file
# 2.process data with line
# 3.send it to influxDB,then next line
# import datetime
from logbook import Logger
import re
import socket
import time
import requests
import requests.exceptions
import datetime
from influxdb import InfluxDBClient

log = Logger('his_post')


# import logbook

# logger = logbook.Logger("mts_his")

class InfluxDBBase(object):
    """ influxdb lib """

    def __init__(self, config):
        """ init """
        log.debug("init InfluxDBBase...")
        # print("init InfluxDBBase...")
        self.config = config["influxdb"]
        self.client = InfluxDBClient(self.config['host'],
                                     self.config['port'], self.config['username'],
                                     self.config['password'], self.config['db'])

    def send(self, json_body):
        """ write point """
        #         # print("influx,", json_body[0]["tags"]["seq"])
        #         # print(json_body)

        self.client.write_points(json_body, time_precision='u')


def get_timestamp(datestr):
    pattern = r"""(\d+)/(\d+)/(\d+) (\d+):(\d+):(\d+)"""
    compile_obj = re.compile(pattern)
    match_obj = compile_obj.search(datestr)
    if match_obj is None:
        return 0
    all_groups = match_obj.groups()
    year, month, day, hour, minute, second = all_groups
    # print all_groups
    dp = {"year": int(year), "month": int(month), "day": int(day),
          "hour": int(hour), "minute": int(minute), "second": int(second)}

    dt = datetime.datetime(**dp)

    return time.mktime(dt.timetuple())


def get_float(f):
    if f == "":
        return 0.0
    else:
        return float(f)


class Outputer(InfluxDBBase):
    def __init__(self, conf, key):
        self.conf = conf
        self.url = conf[key]["config"]["url"]
        self.nodename = conf[key]["config"]["nodename"]
        self.eqpt_no = conf[key]["config"]["eqpt_no"]
        self.seq = 1
        self.HEADERS = {'content-type': 'application/json', 'accept': 'json', 'User-Agent': 'mabo'}
        self.hostname = socket.gethostname()
        self._session = requests.Session()
        super(Outputer, self).__init__(conf)
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

    def message_process(self, msgline, task, measurement, inject_tags):
        error_dict = dict()
        data = msgline.split("\t")
        try:
            timestamp = get_timestamp(data[0])

        except:
            error_dict['time'] = ('http_his_97', data[0])
            return 1, error_dict
        data_len = len(data)
        switch = {3: self.len3, 7: self.len7, 10: self.len10}
        if data_len in [3, 7, 10]:
            try:
                tags, fields = switch[data_len](data, task)
            except:
                error_dict['info'] = ('http_his_102', data)
                return 1, error_dict
            self.seq += 1
            fields = del_null_fields(fields)
            if inject_tags is not None:
                tags.update(inject_tags)
            post_data = {"tags": tags,
                         "fields": fields,
                         "time": 1000000 * int(timestamp) + self.seq % 1000,
                         "measurement": measurement}

            log.debug("process data okay! we'll send it to influxdb")
            log.debug("--" * 20)
            log.debug(fields)
            log.debug("--" * 20)
            log.debug(tags)
            log.debug("--" * 20)

            self.send([post_data])
            return 0, None

        else:
            self.seq += 1
            tags = {"hostname": self.hostname, "node": self.nodename, "task": task, "seq": self.seq}
            if inject_tags is not None:
                tags.update(inject_tags)
            fields = {"line": msgline, "datetime": time.strftime("%Y-%m-%d %H:%M:%S")}

            # measurement: issueline
            post_data = {"tags": tags, "fields": fields, "time": int(time.time()), "measurement": "issueline_rpc"}
            log.debug("issue_line rpc:")
            error_dict['info'] = ('http_his_130', msgline)

            self.send([post_data])
            # with open('data_his.txt', 'wt') as f:
            #     f.write(str(post_data))
            return 1, error_dict

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
    log.debug("del None fields~~~~")
    log.debug(remove_list)
    for key in remove_list:
        del fields[key]
    return fields

