# -*- coding: utf-8 -*-
import pendulum
from influxdb import InfluxDBClient
from logbook import Logger

log = Logger('agilent')


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
        time_str, message = msgline.split('\t\t')
        good_format_time_str = fill_zero(time_str)
        timestamp = pendulum.from_format(good_format_time_str, '%Y-%m-%d %H:%M:%S', 'Asia/Shanghai').timestamp
        status = get_status(message)
        fields = {'status': status, 'message': message}
        payload = {'fields': fields,
                   'time': 1000000 * timestamp + self.seq % 1000,
                   'measurement': measurement,
                   'tags': {
                       'eqpt_no': self.eqpt_no,
                       'nodename': self.nodename}
                   }
        self.send([payload])
        self.seq += 1
        log.debug('sent!')
        return 0, None


def fill_zero(time_str):
    date, clock = time_str.split(' ')
    return ' '.join(('-'.join([one.zfill(2) for one in date.split('-')]),
                     ':'.join([one.zfill(2) for one in clock.split(':')])))


def get_status(message):
    status = None
    if message.endswith('started'):
        status = 1
    if message.endswith('finished'):
        status = 0
    return status
