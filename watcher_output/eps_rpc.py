# -*- coding: utf-8 -*-
import pendulum
import socket
from logbook import Logger
from influxdb import InfluxDBClient

log = Logger('eps_rpc')


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
        self.seq = 1
        self.hostname = socket.gethostname()
        super(Outputer, self).__init__(conf)

    def message_process(self, msgline, task, measurement, inject_tags):
        date_temp, str_temp, log_msg = msgline.split(' ', 2)
        time_str = date_temp + str_temp + log_msg[:2]
        timestamp = pendulum.from_format(time_str, '%m/%d/%Y%H:%M:%S%p', 'Asia/Shanghai').timestamp
        self.seq += 1
        temp = log_msg[3:].split('\t')
        unknown_1, unknow_2, unknow_3 = temp[0], temp[1], temp[-1]

        fields = {'unknown_1': unknown_1, 'unknown_2': unknow_2, 'unknown_3': unknow_3}
        tags = {'eqpt_no': self.eqpt_no, 'nodename': self.nodename, 'task': task}
        if inject_tags is not None:
            tags.update(inject_tags)
        post_data = {"tags": tags,
                     "fields": fields,
                     "time": 1000000 * int(timestamp) + self.seq % 1000,
                     "measurement": measurement}
        self.send([post_data])
        log.debug('eps_rpc sent!')
        return 0, None
