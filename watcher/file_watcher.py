# -*- coding: utf-8 -*-
"""A module for getting line logs


This moudule contains a class Watcher. This class will do following things:
    1.finding all files under moniterd directory.
    2.get contents from qualified files.
    3.pass the line to process pluing.

"""
import json
import os
import re
import time
import traceback
import logbook

log = logbook.Logger("watcher")


class Watcher(object):
    """ logstream watcher """

    def __init__(self, item, app_conf):
        """ init """
        watcher_conf = app_conf['Logstreamer'][item]
        self.error_count = 0  #
        self.item = item
        self.rescan_interval = int(watcher_conf["rescan_interval"][:-1])
        self.ticker_interval = watcher_conf["ticker_interval"]
        self.log_directory = watcher_conf["log_directory"]
        self.file_match_pattern = watcher_conf["file_match"]
        self.task_position = watcher_conf["task_position"]
        self.ext = ".jrn"
        self.matched_files = []
        self.oldest_duration = watcher_conf.get('oldest_duration', "31d")
        self.measurement = watcher_conf["measurement"]
        # key is the one of the python plugin. s.t : MTSHisOutput   MTSLogOutput
        self.processer = get_processor(watcher_conf['processor'], app_conf)
        self.logstreamer = "logstreamer"
        check_path("logstreamer")

    def set_seek(self, file_name, seek):
        """

        :param file_name: absolute file name
        :param seek: the point to file we last read stop at.
        :return:no sense for now
        """
        journey_file = os.sep.join([self.logstreamer, self.item + "_" + file_name + self.ext])
        if os.path.exists(journey_file) and seek == 0:
            return 1
        full_path = os.path.abspath(file_name)
        seekinfo = {"seek": seek, "file_name": full_path}
        jsonstr = json.dumps(seekinfo)
        with open(journey_file, "w") as f:
            f.write(jsonstr)

    def get_seek(self, file_name):
        """get seek number from journey file

        :param file_name: absolute file name
        :return: seek number or 0
        """

        self.logstreamer = "logstreamer"

        fn = os.sep.join([self.logstreamer, self.item + "_" + file_name + self.ext])

        if os.path.exists(fn):

            with open(fn, "r") as fileh:
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
            log.debug('filename matched {} files'.format(len(files)))
            # filter files
            self.matched_files = self.filter(files)

            log.debug('time range matched {} files'.format(len(self.matched_files)))

            # wating some seconds updates.
            time.sleep(self.rescan_interval)

    def _watch(self):
        """

        :return:
        """
        for filename in self.matched_files:
            contents = self.read_contents(filename)
            if contents == 'pass':
                name = os.path.basename(filename)
                log.debug('{} read all'.format(name))
                continue

            task = Watcher.get_task_name(self.task_position, filename)
            try:
                for line in contents.split('\n'):
                    message = line.strip()
                    if message == "":
                        pass
                    else:
                        # TODO: inject_tag use should be fixed.
                        rtn, error_dict = self.processer.message_process(message, task,
                                                                         self.measurement, inject_tags=None)
                        if rtn != 0:
                            log.error(error_dict)

                            # raise (Exception("msg processer has issue"))
                    self.error_count = 0
            except Exception as e:
                self.error_count += 1
                log.error(e)
                log.error(traceback.format_exc())
                if self.error_count > 3:
                    time.sleep(30)

    def watch(self):
        """real worker
        if the collector didn't work timely(of course he is a lazy guy),
        i wait him for one sencond ,and then do my job.
        """
        while True:
            try:
                self._watch()
            except Exception as ex:
                log.error(traceback.format_exc())
                log.error(ex)
            time.sleep(self.ticker_interval)

    def read_contents(self, filename):
        """from the seek point to read file.

        read seek number from journey file,then set the seek on file
        and read the rest contents of files.

        :param filename:a log file name
        :return:the contents we don't process.
        """
        fn = os.path.basename(filename)
        file_size = os.stat(filename)[6]

        present_point = self.get_seek(fn)
        # FIXME:this is for debug !!!
        # present_point = 0
        if file_size == present_point:
            return 'pass'
        with open(filename, 'r') as for_read:
            for_read.seek(present_point)
            contents = for_read.read(file_size - present_point)
        present_point = file_size
        self.set_seek(fn, present_point)
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
            log.debug('task name is {}'.format(task))
            return task[:-4]
        return task


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
