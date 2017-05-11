import re
import socket

import pendulum
from influxdb import InfluxDBClient
from logbook import Logger

log = Logger('Fidia_post')


class InfluxDBBase(object):
    """ influxdb lib """

    def __init__(self, config):
        """ init """
        log.debug("init InfluxDBBase...")
        self.config = config["influxdb"]
        self.client = InfluxDBClient(self.config['host'],
                                     self.config['port'], self.config['username'],
                                     self.config['password'], self.config['db'])

    def send(self, json_body):
        """ write point """
        return self.client.write_points(json_body, time_precision='u')


class Outputer(InfluxDBBase):
    def __init__(self, conf, processor):
        processor_conf = conf['processor'][processor]
        self.seq = 0
        self.hostname = socket.gethostname()
        self.nodename = processor_conf["config"]["nodename"]
        self.eqpt_no = processor_conf["config"]["eqpt_no"]
        self.level = processor_conf['level']
        rawstr = r"""L O G    F I L E    C R E A T E D    O N   << (.*) >>"""
        self.compile_obj = re.compile(rawstr)
        self.current_date = None
        super(Outputer, self).__init__(conf)
        # TODO: sooner or later ,i will clean this complicated inition.

    def get_date(self, line):
        # common variables
        match_obj = self.compile_obj.search(line)
        if match_obj:
            group_1 = match_obj.group(1)
            d = pendulum.from_format(group_1, '%d-%b-%Y')
            self.current_date = d.to_date_string()

    def message_process(self, msg_line, task, measurement, inject_tags):
        self.seq += 1
        self.get_date(msg_line)
        start_character = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        str_character = tuple((str(i) for i in start_character))
        try:
            if msg_line.startswith(str_character):
                data = msg_line.split(' ', 3)
                _, time, code, message = data
                status = Outputer.calculate_status(message)
                level = Outputer.calculate_level(code)
                data = status, level, message, code
                payload = self.construct_json(time, data, measurement, task="zan shi wei ding")
            else:
                return 0, None
        except Exception as e:
            return 0, None

        # through this function,we can append extra info from the conf files.

        if self.send([payload["data"]]):
            log.debug('data is transmitted')

        return 0, None

    def construct_json(self, time, data, measurement, task):
        fields = {"Msg": data[2],
                  "status": data[0],
                  "Code": data[-1],
                  "Level": self.level[data[1]]}

        tags = {
            "node": self.nodename,
            "task": task,
            "seq": self.seq,
            "eqpt_no": self.eqpt_no, }
        dt_str = "{}T{}".format(self.current_date, time)
        dt = pendulum.from_format(dt_str, '%Y-%m-%dT%H:%M:%S', 'Asia/Shanghai')
        payload = {"data": {"tags": tags,
                            "fields": fields,
                            "time": int(dt.timestamp) * 1000000 + self.seq % 1000,
                            "measurement": measurement}}
        return payload

    @staticmethod
    def calculate_status(message):
        status_mark = {
            'PRESSED THE START KEY': 1,
            'IDHLD FEED HOLD': 0,
            'MTE: TABLE SAFETY DOORS UNLATCHED': 1,
            'ACCEPTED COMMAND: ABORT ACCEPTED': 0

        }
        for mark in status_mark:
            if message.startswith(mark):
                return status_mark[mark]
        return None

    @staticmethod
    def calculate_level(code):
        level_dict = {'F': 'Fatal', 'R': 'Request',
                      'E': 'Error', 'W': 'Warning',
                      'I': 'Information', 'D': 'Debug'}
        level = level_dict.get(code[0])
        return level
