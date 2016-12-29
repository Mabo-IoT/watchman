# -*- coding: utf-8 -*-
import pendulum
from logbook import Logger
from influxdb import InfluxDBClient

log = Logger('tts_crtlmessage')


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

        self.client.write_points(json_body, time_precision='u')


class Outputer(InfluxDBBase):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.conf = conf
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.status_map = config_conf['status']
        self.seq = 1

        super(Outputer, self).__init__(conf)

    def message_process(self, msgline, task, measurement, inject_tags):

        log_list = [one.strip(' ') for one in msgline.split(' ', 1)]
        if len(log_list) == 1:
            return 0, None
        else:
            code, message = log_list

        status = self.get_status(code)
        timestamp = pendulum.now('Asia/Shanghai').timestamp
        payload = {'fields': {'status': status, 'code': code, 'message': message},
                   'time': 1000000 * timestamp + self.seq % 1000,
                   'measurement': measurement,
                   'tags': {'eqpt_no': 'pec0_xiaoma', 'nodename': 'tts_11'}}
        self.send([payload])
        log.debug('sent!')
        return 0, None

    def get_status(self, code):
        for one in self.status_map:
            if code[0].lower().startswith(one[0]):
                return self.status_map[one]


