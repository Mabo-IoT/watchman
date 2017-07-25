# -*- coding: utf-8 -*-
"""
1. monitor specified file size.
2. if size is less than program hold, delete and refresh.
"""
import os
import time

FILENAME = 'test'
REMOVE_PATH = 'remove'


class MonitorFile(object):
    def __init__(self):
        self.last_filesize = 0
        pass

    def watch(self):
        file_size = os.stat(FILENAME)[6]
        if file_size >= self.last_filesize:
            self.last_filesize = file_size
            print 'not'
            return 'not'
        else:
            self.last_filesize = file_size
            print 'remove'
            return 'remove'

    def reomove(self):
        os.remove(REMOVE_PATH)

        pass

    def run(self):
        while True:
            res = self.watch()

            if res == 'remove':
                self.reomove()
            time.sleep(1)


if __name__ == '__main__':

    monitor = MonitorFile()

    monitor.run()
