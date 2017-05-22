# -*- coding: utf-8 -*-
import re
import time

from logbook import Logger

log = Logger("eps_station")


class MsgParser(object):
    """
    msgline parser.
    """

    def __init__(self):
        rawstr = r'^\((.*)\) (.*) \[(.*)\] "(.*)"'
        self.compile_obj = re.compile(rawstr)


class Outputer(object):
    def __init__(self, conf, processor):
        config_conf = conf['processor'][processor]['config']
        self.seq = 0
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]

        # get a parser.
        self.parser = MsgParser()

        # for status.
        self.status = None
        self.status_map = {'running': 1, 'error': 2, 'stop': 0, 'unknown': 3}
        self.status_set = conf['processor'][processor]['status']

        # for level
        self.level = conf['processor'][processor]['level']

        # for recording
        self.last_timestamp = 0

    def message_process(self, msgline, task, measurement):

        not_valid = Outputer.check_is_not_valid(msgline)
        if not_valid:
            influx_json = {
                "fields": {'msg': msgline},
                "time": int(time.time()) * 1000000,
                "measurement": 'new_issueline'}
            return 1, 'wrong format.', influx_json

        some_data = self.process_data(msgline)
        if some_data is None:
            influx_json = {
                "fields": {'msg': msgline},
                "time": 1000000 * int(time.time()),
                "measurement": 'new_issueline'}
            return 1, 'wrong format.', influx_json

        data = self.build_fields(some_data)

        # construct influx json.
        if "status" in data:
            fields = {
                "status": data["status"],
                "Msg": data["msg"],
                "logger": data["logger"],

                "task": task,
                "seq": self.seq,
                "FLevel": data["level"]}
        else:
            fields = {
                "Msg": data["msg"],
                "logger": data["logger"],

                "task": task,
                "seq": self.seq,
                "FLevel": data["level"]

            }
        tags = {
            "Level": data["level"],
            "eqpt_no": self.eqpt_no,
        }

        influx_json = {"tags": tags,
                       "fields": fields,
                       "time": 1000000 * int(data["time"]) + self.seq % 1000,
                       "measurement": measurement}

        return 0, 'process successful', influx_json

    @staticmethod
    def check_is_not_valid(msgline):

        if msgline.startswith('('):
            return False
        else:
            log.info('unexpected msg_line, pass.')
            return True

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
                self.status = 2
        else:
            self.status = None

        # for recoding how long between this message and the former.
        if self.last_timestamp == 0:
            self.last_timestamp = timestamp
            duration = 0
        else:
            duration = timestamp - self.last_timestamp
            self.last_timestamp = timestamp

        # build message
        data = {
            "duration": duration, "time": timestamp,
            "logger": logger_name, "level": level, "msg": log_msg,
            "status": self.status}
        if self.status is None:
            del data["status"]

        return data

    @staticmethod
    def calculate_status(log_msg, status_set):
        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return status
        return None


def test():
    import toml
    with open("watchman.toml") as conf_file:
        config = toml.loads(conf_file.read())
    out_tester = Outputer(config, 'MTSLogOutput')
    pth = "C:\\MTS 793\\Controllers\\PATAC_FT\\Config\\"
    logs = ['Spindle_Direct_38401000.log', 'Spindle_Matrix_38401.log', 'Spindle_Matrix_with LR000.log',
            'Spindle_Matrix_with LR001.log']
    i = 0
    for logfile in logs:
        # print "this is log {0}".format(i + 1)
        # print"log name :{0}".format(logfile)
        # print "**" * 30
        if i == 4:
            break
        fh = open(pth + logfile, 'r')
        i = i + 1
        for msgline in fh:
            out_tester.message_process(msgline, 'task', 'MTS_Station', )


if __name__ == "__main__":
    test()
