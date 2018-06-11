# -*- coding: utf-8 -*-
import re

import pendulum
from logbook import Logger

log = Logger("AVL_alarm")


class Outputer:
    def __init__(self, conf, processor):
        processor_conf = conf['processor'][processor]

        # eqpt_info
        self.nodename = processor_conf["config"]["nodename"]
        self.eqpt_no = processor_conf["config"]["eqpt_no"]
        self.seq = 0

    def message_process(self, msgline, task, measurement):
        """接受一条日志，解析完成后，取得相应信息，组成influxdb
        所需字典，返回。

        :param msgline: 日志
        :param task: 本次实验任务名
        :param measurement: 此实验的表名
        :return: 以influxdb line protocal 组成的字典
        """
        if check_valid(msgline):

            #fields = dict()
            #data = dict()
            sys_data = msgline.split(';', 3)
            j = sys_data

            if len(j) > 3:

                if j[0] == '(A)':

                    alarm_status = 1
                    timestamp = int((pendulum.from_format(j[1] + ' ' + j[2], '%Y-%m-%d %H:%M:%S.%f','Asia/Shanghai').float_timestamp)*1000000)
                    alarm_info = ';'.join([x.strip() for x in j[3].split(';')])

                elif re.match('.+SYSTEM OK.+', j[3]) or re.match('AutomationSystemController.ASC;M A N U A L.+',j[3]):

                    alarm_status = 0
                    timestamp = int((float(pendulum.from_format(j[1] + ' ' + j[2], '%Y-%m-%d %H:%M:%S.%f','Asia/Shanghai').float_timestamp))*1000000)
                    alarm_info = 'No Error!'

                else:
                    return 2, 'wrong format', None

                tags = { "eqpt_no": self.eqpt_no}
                fields = {"alarm_status":alarm_status,"alarm_info":alarm_info}
                influx_json = {"tags": tags,
                               "fields": fields,
                               "time": timestamp,
                               "measurement": measurement}
                return 0, 'process successful', influx_json
            return 2,'wrong format',None
        else:
            return 2, 'wrong format', None

def check_valid(msgline):
    judge_key = '.+;.+;.+'
    if re.match(judge_key, msgline):
        return True
    else:
        return False
