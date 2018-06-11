# -*- coding: utf-8 -*-
"""A module for getting line logs


This moudule contains a class Watcher. This class will do following things:
    1.finding all files under moniterd directory.
    2.get contents from qualified files.
    3.pass the line to process pluing.

"""
import codecs
import json
import os
import re
import time
import traceback
from sys import version_info

import logbook
import msgpack

from marshall_io.databse_io import RedisWrapper, InfluxdbWrapper

log = logbook.Logger("file_watcher")


class Watcher(object):
    """ logstream watcher """

    def __init__(self, item, app_conf):
        """ init """
        watcher_conf = app_conf['Logstreamer'][item]
        self.error_count = 0  #
        self.item = item
        self.rescan_interval = int(watcher_conf.get("rescan_interval", '20s')[:-1])
        self.ticker_interval = watcher_conf.get("ticker_interval", 1)
        self.log_directory = watcher_conf["log_directory"]
        self.file_match_pattern = watcher_conf["file_match"]

        self.ext = ".jrn"
        self.matched_files = []
        self.oldest_duration = watcher_conf.get('oldest_duration', "1d")
        self.measurement = watcher_conf["measurement"]
        self.user_tag = watcher_conf['user_tag']

        # key is the one of the python plugin. s.t : MTSHisOutput   MTSLogOutput
        self.processer = get_processor(watcher_conf['processor'], app_conf)

        self.logstreamer = "read_recorder"

        # output tools
        self.output = app_conf['data_output']['which']

        # redis initialize.
        if self.output == 'redis':
            self.redis = RedisWrapper(app_conf['data_output']['redis'])
            self.redis.script_load(app_conf['data_output']['redis']['lua'])

        # influxdb initialize
        if self.output == 'influxdb':
            self.influx_conf = app_conf['data_output']['influxdb']
            self.influx = InfluxdbWrapper(app_conf['data_output']['influxdb'])
            self.time_precision = self.influx_conf['time_precison']

        # task infomation
        self.task_infomation = watcher_conf['task_position']

        check_path("read_recorder")

    def re_connect_influxdb(self):
        # influxdb initialize
        if self.output == 'influxdb':
            self.influx = InfluxdbWrapper(self.influx_conf)

    def set_seek(self, file_name, seek):
        """

        :param file_name: absolute file name
        :param seek: the point to file we last read stop at.
        :return:no sense for now
        """

        some = file_name.replace('/', '-').replace(':', '-')
        journey_file = os.sep.join([self.logstreamer, self.item + "-(" + some + ')' + self.ext])

        if os.path.exists(journey_file) and seek == 0:
            return 1

        file_name = to_single_slash(file_name)
        # construct seek info
        seekinfo = {'seek': seek, 'file_name': file_name,
                    'location': to_single_slash(os.path.dirname(file_name))}

        jsonstr = json.dumps(seekinfo)
        with open(journey_file, "w") as f:
            f.write(jsonstr)

    def get_seek(self, file_name):
        """get seek number from journey file

        :param file_name: absolute file name
        :return: seek number or 0
        """

        self.logstreamer = "read_recorder"
        base_name = os.path.basename(file_name)
        log.debug(file_name)
        some = file_name.replace('/', '-').replace(':', '-')
        log.debug(self.item)
        log.debug(some)
        fn = os.sep.join([self.logstreamer,self.item + "-(" + some  + ')' + self.ext])

        if os.path.exists(fn):

            with open(fn, "r") as fileh:
                fileh.seek(0)
                journey_dict = json.loads(fileh.read())
            return journey_dict["seek"]
        else:
            self.set_seek(file_name, 0)
            return 0

    def collector(self):
        """ Scan and update matched file list

        find specified type files with specified directory
        filter files with last modified time.

        """
        while True:
            # find all files with file_patern under log_directory
            files = Watcher.find_files(self.log_directory, self.file_match_pattern)
            log.info('filename matched {} files.'.format(len(files)))
            # filter files
            self.matched_files = self.filter(files)

            log.info('time range matched {} files.'.format(len(self.matched_files)))

            # wating some seconds updates.
            log.info('wating {}s for re scanning.'.format(self.rescan_interval))
            time.sleep(self.rescan_interval)

    def _watch(self):
        """
        私有函数
        按照journey文件的位置读取相应的日志文件内容

        调用message_process方法进行真正的解析。
        message： 日志文件的每一行
        task： 这条日志隶属于什么任务
        measurement： 会传递到哪张表

        :return: None
        """
        for filename in self.matched_files:
            present = self.get_seek(filename)

            contents = self.read_contents(filename)
            if contents == 'pass':
                name = os.path.basename(filename)
                continue

            for line in contents.split('\n'):
                message = line.strip()

                if message == "":
                    continue
                else:
                    task_ralated = (filename, self.task_infomation)
                    process_rtn, info, influx_json = self.processer.message_process(message, task_ralated,
                                                                                    self.measurement, )
                    if process_rtn == 1:
                        log.error(info)
                        self.send(influx_json, method=self.output)
                        # for debuging use
                        # with open('D:\work\work_stuff\log\mts\something_wrong.txt', 'a') as f:
                        #     f.write(message+'\n'+filename+'\n-----------------\n')

                    if process_rtn == 0:
                        influx_json['tags'].update(self.user_tag)
                        self.send(influx_json, method=self.output)

                    if process_rtn == 2:
                        pass
                present += len(line)
                if present > os.stat(filename)[6]:
                    self.set_seek(filename,0)
                else:
                    self.set_seek(filename,present)

    def watch(self):
        """读log日志，分行交给processor处理

        """
        while True:
            try:
                self._watch()
            except Exception as ex:
                log.error('error in reading log file... ')
                log.error(traceback.format_exc())
                log.error(ex)
            log.info('所有匹配log日志已经传输完毕, All is Done!')
            log.info('wating{}s,re processing log'.format(self.ticker_interval))
            time.sleep(self.ticker_interval)

    def read_contents(self, filename):
        """from the seek point to read file.

        read seek number from journey file,then set the seek on file
        and read the rest contents of files.

        :param filename:a log file name
        :return:the contents we don't process.
        """
        fn = filename
        file_size = os.stat(filename)[6]

        present_point = self.get_seek(fn)

        #present_point = 0  # FIXME:this is for debuging !!!
        if file_size == present_point:
            log.debug('file_size = present_point')
            return 'pass'

        if version_info[0] == 2:
            with codecs.open(filename, 'r', errors='ignore') as for_read:
                for_read.seek(present_point)
                contents = for_read.read(file_size - present_point)
        else:
            with open(filename, 'r', errors='ignore') as for_read:
                for_read.seek(present_point)
                contents = for_read.read(file_size - present_point)

        #present_point = file_size
        # self.set_seek(fn, present_point)

        return contents

    def filter(self, files):
        """

        :param files: file name list
        :return: name and time matched file list
        """
        oldest_duration_seconds = get_duration_sec(self.oldest_duration)
        now = time.time()
        filtered = []
        for file in files:
            last_modified_time = os.stat(file)[8]
            if now - last_modified_time < oldest_duration_seconds:
                filtered.append(file)
        return filtered

    @staticmethod
    def find_files(log_directory, file_pattern):
        """Traverse directory,get all log files.

        :param log_directory: the directory on which we want to monitor
        :param file_pattern: regular expression of log file
        :return: all name matched files.
        """
        log_file = []
        for each in log_directory:
            for root, dirs, files in os.walk(each):
                for one_file in files:
                    if re.search(file_pattern, one_file):
                        log_file.append(root + os.sep + one_file)
        return log_file

    @staticmethod
    def get_task_name(position, filename):
        filename_split = filename.split(os.sep)
        task = filename_split[position]
        if '.log' in task:
            log.info('task name is {}'.format(task[:-4]))
            return task[:-4]
        return task

    def send(self, json_data, method):
        if method == 'redis':
            timestamp, fields, measurement, tags, eqpt_no = json_data['time'], json_data['fields'], json_data[
                'measurement'], json_data['tags'], json_data['tags']['eqpt_no']
            timestamp = pack_to_byte(timestamp)
            fields = pack_to_byte(fields)
            measurement = pack_to_byte(measurement)
            tags = pack_to_byte(tags)
            eqpt_no = pack_to_byte(tags)

            unit = 'u'
            unit = pack_to_byte(unit)

            lua_info = self.redis.enqueue(timestamp=timestamp, tags=tags,
                                       fields=fields, measurement=measurement, unit=unit)
            log.info('send data to redis,{}'.format(lua_info))
            return 0
        if method == 'influxdb':
            try:
                info = self.influx.send([json_data], time_precision=self.time_precision)
                log.info('send data to inflxudb.{}, {}'.format(json_data['measurement'], info))

            except Exception as e:
                log.error(e)
                log.error('remote server error')
            return 0


def to_single_slash(filename):
    path = filename.split('\\')
    connect = '/'
    filename = connect.join(path)
    return filename


def pack_to_byte(raw):
    return msgpack.packb(raw)


def check_path(logstreamer_path):
    if not os.path.exists(logstreamer_path):
        os.mkdir(logstreamer_path)


def get_duration_sec(duration):
    """Standardlize the time unit.

    s : second, m : minutes, h : hour, d : day, w : week

    :param duration: time string eg: '15s' '30'
    :return: time seconds.

    """

    d = {"s": 1, "m": 60, "h": 60 * 60, "d": 60 * 60 * 24, "w": 60 * 60 * 24 * 7}
    quant = int(duration[:-1])
    unit = duration[-1]
    if unit in ["m", "h", "w", "d"]:
        x = quant * d[unit]
        return x
    else:
        raise (Exception("wrong time unit"))


def get_processor(processor, app_conf):
    """Get Outputer class from plugin.

    get Outputer class from plugin (fidia, st, kc, rpc or extra.)

    :param processor: a tring of which plugin we use.
    :param app_conf: a dict of application conf files.
    :return: return Outputer class from plugin.

    """
    processor_conf = app_conf['processor'][processor]

    plugin_name = processor_conf['processor_path']
    classname = processor_conf["class"]

    mod = __import__(plugin_name, fromlist=[classname])
    klass = getattr(mod, classname)

    return klass(app_conf, processor)
