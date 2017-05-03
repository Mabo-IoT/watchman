# -*- coding: utf-8 -*-
"""
>>> with open("test_conf.toml") as conf_file:
...     conf_file = toml.loads(conf_file.read())
>>> red = RedisClient(conf_file)
>>> print(red)
RedisClient use the  conf
"""
import traceback

import redis
import toml
from influxdb import InfluxDBClient
from logbook import Logger

log = Logger('redis_client')


class RedisClient(object):
    """ redis client """

    def __init__(self, conf):
        """ init """

        self.conf = conf

        self.connect()

    def __repr__(self):
        return 'RedisClient use the  conf'

    def connect(self):

        try:
            # make a pool to connect
            pool = redis.ConnectionPool(host=self.conf["host"],
                                        port=self.conf["port"],
                                        db=self.conf["db"])

            self.db = redis.Redis(connection_pool=pool)

        except Exception as e:
            log.error(e)
            log.error(traceback.format_exc())
            pass

    def load_script(self, enqueue_script_filename):
        """ load script """

        with open(enqueue_script_filename, "r") as fh:
            enqueue_script = fh.read()

            # 载入lua脚本，返回此lua脚本的SHA值。
            self.SHA = self.db.script_load(enqueue_script)

    def enqueue(self, eqpt_no, timestamp, tags, fields, measurement):
        """ enqueue """

        script_result = self.db.evalsha(self.SHA, 1, eqpt_no, timestamp, tags, fields, measurement)

        return script_result

    def get_len(self, key_name):
        return self.db.llen(key_name)

    def dequeue(self, key_name):
        """ lpop """
        return self.db.lpop(key_name)

    def re_queue(self, key_name, data):
        """ re queue """

        self.db.lpush(key_name, data)


class InfluxDBBase(object):
    """ influxdb lib """

    def __init__(self, config):
        """ init """
        log.debug("init InfluxDBBase...")
        self.client = InfluxDBClient(config['host'],
                                     config['port'], config['username'],
                                     config['password'], config['db'])

    def write(self, data_dict):
        self.client.write(data_dict)

    def send(self, json_body):
        """ write point """

        return self.client.write_points(json_body, time_precision='u')


if __name__ == "__main__":
    import doctest

    doctest.testmod()
