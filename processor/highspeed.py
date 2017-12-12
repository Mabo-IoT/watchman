# -*- coding: utf-8 -*-

from logbook import Logger

log = Logger("mts_station_mpt")


class Outputer:
    def __init__(self, conf, processor):
        pass

    def message_process(self, msgline, task, measurement):
        """接受一条日志，解析完成后，取得相应信息，组成influxdb
        所需字典，返回。

        :param msgline: 日志
        :param task: 本次实验任务名
        :param measurement: 此实验的表名
        :return: 以influxdb line protocal 组成的字典
        """
        msgline.split

        return 0, 'process successful', _
