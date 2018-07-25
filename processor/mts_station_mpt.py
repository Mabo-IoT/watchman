# -*- coding: utf-8 -*-
import os
import re
import time
import redis

from logbook import Logger

log = Logger("mts_station_mpt")


class msg_parser(object):
    """
    msgline parser.
    """

    def __init__(self):
        rawstr = r'^\((.*)\) (.*) \[(.*)\] "(.*)"'
        self.compile_obj = re.compile(rawstr,flags=re.M)


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.seq = 0
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]

        # get a parser.
        self.parser = msg_parser()

        # for status.
        self.status = 0
        self.status_map = {'running': 1, 'error': 2, 'stop': 0, 'unknown': 3}
        self.status_set = conf['processor'][processor]['status']
        self.last_status = self.status

        # for level
        self.level = conf['processor'][processor]['level']

        # for recording
        self.last_timestamp = 0

    def message_process(self, msgline, task_related, measurement,redis_db):
        """接受一条日志，解析完成后，取得相应信息，组成influxdb
        所需字典，返回。

        :param msgline: 日志
        :param task_related: 相关信息
        :param measurement: 此实验的表名
        :return: 以influxdb line protocal 组成的字典
        """
        log.debug(redis_db)
        some_data = self.process_data(msgline)
        data = self.build_fields(some_data)
        filename,task_position = task_related
        task = self.get_task(filename)
        # construct influx json.
        pools = redis.ConnectionPool(host='127.0.0.1', port=6379, db=redis_db, decode_responses=True)
        rr = redis.Redis(connection_pool=pools)

        if "status" in data:
            status = self.get_status(data["status"],task,redis_db)
        fields = {
            "Msg": data["msg"],
            "logger": data["logger"],

            "task": task,

            "FLevel": data["level"]

        }
        tags = {
            "Level": data["level"],
            "eqpt_no": self.eqpt_no,
        }

        # seq value.
        if rr.llen(task) == 0:
            seq = 0
            rr.rpush(task,data["time"])
        else:
            if data["time"] == float(rr.lrange(task,-1,-1)[0]):
                rr.rpush(task,data["time"])
                seq = rr.llen(task) - 1
                log.debug(rr.llen(task))
            else:
                rr.delete(task)
                rr.rpush(task,data["time"])
                seq = 0
        
        influx_json = {"tags": tags,
                       "fields": fields,
                       "time": 1000000 * int(data["time"]) + seq % 1000,
                       "measurement": measurement}

        return 0, 'process successful', influx_json

    @staticmethod
    def calculate_status(log_msg, status_set):
        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return status
        return None

    @staticmethod
    def get_status(task_status,task,redis_db):
        pools = redis.ConnectionPool(host='127.0.0.1', port=6379, db=redis_db, decode_responses=True)
        rr = redis.Redis(connection_pool=pools)
        if task_status == 0:
            rr.srem('status', task)
        elif task_status == 1:
            rr.sadd('status', task)
        if task_status == None:
            return None
        if rr.scard('status') == 0:
            return 0
        else:
            return 1

    def build_fields(self, *args):
        """get raw message, return food influx_json.
        :param args:
        :return: dict, influx_json
        """
        timestamp, level_msg, logger_name, log_msg = args[0]

        level = self.level.get(level_msg, 8)

        str_status = Outputer.calculate_status(log_msg, self.status_set)

        if str_status is not None:
            self.status = self.status_map[str(str_status)]
            if level == 3:
                self.status = 0
        else:
            self.status = None

        # build message
        data = { "time": timestamp, "logger": logger_name, "level": level, "msg": log_msg,"status": self.status}
        if self.status is None:
            del data["status"]

        return data

    def process_data(self, msgline):
        is_match = self.parser.compile_obj.search(msgline)
        if is_match is not None:
            time_str, level_msg, logger_name, log_msg = all_groups = is_match.groups()

            # make time_stamp
            logtime = time.strptime(time_str, '%m/%d/%Y %H:%M:%S')
            timestamp = time.mktime(logtime)

            # exclude no used info
            log_msg = log_msg.strip('"')

            return timestamp, level_msg, logger_name, log_msg
        else:
            return None


    def get_task(self, file_absolute_path, ):

        some = -1
        if isinstance(some, int):
            log.debug(file_absolute_path)
            name = file_absolute_path.split(os.sep)[some]
            # if .xxx ,drop it
            if some == -1:
                name = name[:-4]
            task = name
        else:
            task_map = dict([tuple(one.split(':')) for one in some])
            path_split = file_absolute_path.split(os.sep)

            for one in task_map:
                if one in path_split:
                    task = task_map[one]
                    return task
            task = 'full'
        return task
