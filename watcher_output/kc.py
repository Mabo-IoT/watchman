# -*- coding: utf-8 -*-

import pendulum
from logbook import Logger

log = Logger("mts_station_mpt")


class Outputer:
    def __init__(self, conf, processor):

        config_conf = conf['processor'][processor]['config']
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.status_set = conf['processor'][processor]['status']
        self.status_mapping = {'run': 1, 'stop': 0}
        pass

    def message_process(self, msgline, task, measurement):
        """接受一条日志，解析完成后，取得相应信息，组成influxdb
        所需字典，返回。

        :param msgline: 日志
        :param task: 本次实验任务名
        :param measurement: 此实验的表名
        :return: 以influxdb line protocal 组成的字典
        """

        log_list = msgline.split(' ', 5)  # separate message and time
        time_str = ''.join(log_list[:4])
        time = pendulum.from_format(time_str, '%H:%M:%S%b%d%Y', 'Asia/Shanghai').int_timestamp

        message = log_list[-1]

        status = self.get_status(message)
        script_name = Outputer.get_script_name(message)

        influx_json = self.construct_json(time, message, measurement,
                                          status, script_name, task=task)
        # log.debug(influx_json)

        return 0, 'process successful', influx_json

    def construct_json(self, *args, **kwargs):
        """
        time, message, item, inject_tags, status, task):
        :param args: 
        :param kwargs: 
        :return: 
        """
        time, message, measurement, status, script_name = args
        task = kwargs['task']
        fields = {"Msg": message,
                  "status": status,
                  'script_name': script_name}

        tags = {
            "node": self.nodename,
            "task": task,
            "eqpt_no": self.eqpt_no, }

        influx_json = {'tags': tags,
                       'fields': fields,
                       'time': time,
                       "measurement": measurement}

        return influx_json

    def get_status(self, log_msg):
        status_set = self.status_set
        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return self.status_mapping[status]
        return None

    @staticmethod
    def get_script_name(log_msg):
        if 'script' in log_msg:
            script_name = log_msg.split(' ', 5)[-1].split('/')[-1].split('.')[0]
            return script_name
