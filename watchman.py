# -*- coding: utf-8 -*-
"""A application for processing log files.

This application will transfer log file into stream line, and
process it , and output status , many filed and extra infomation
the line has.

__author__ = "Marshall"
__copyright__ = "Marshall"
__version__ = "0.0.0"
"""

import os
import threading

from logbook import Logger

from marshall_io.read import get_conf
from marshall_io.setup_logger import setup_logger
from watcher.file_watcher import Watcher

log = Logger('watchman')


def start_app(app_conf):
    """

    :param app_conf: a dict of applcaton conf file.
    """
    logstreamers = app_conf['Logstreamer']
    for logstreamer in logstreamers:
        start_watcher(logstreamer=logstreamer, conf=app_conf)


def start_watcher(**kwargs):
    """

    :param kwargs:
    :return:
    """

    watcher = Watcher(kwargs['logstreamer'], kwargs['conf'])

    watch_t = threading.Thread(target=watcher.watch, name="watch-%s" % kwargs['logstreamer'], args=[])
    watch_t.setDaemon(True)
    watch_t.start()

    log.info('monitoring this direcotory.')
    timer_t = threading.Thread(target=watcher.collector, name="timer-%s" % kwargs['logstreamer'], args=[])
    timer_t.setDaemon(True)
    timer_t.start()

    log.info('collector is working on it.')


def main():
    """ A start poiont of watchman
     do three things:
        1.get conf dict.
        2.setup a logbook configuration.
        3.start worker.
    """
    app_name = os.path.basename(__file__).split('.')[0]
    conf = get_conf('conf/%s.toml' % app_name)
    log_conf = conf['log']
    setup_logger(log_conf)
    start_app(conf)
    while True:
        pass


if __name__ == "__main__":
    main()
