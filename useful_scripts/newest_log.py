"""
Creator: Marshall Fate
Date: 2017/3/15
Time: 12:04

this cript is used for finding the newest log file 's directory
so ,we don't need to find it manually.
for organized as option script. command line option.

1. 罗列所有txt文件，文本类型文件
2. 按修改时间排序
3. 按照 文件名 进行排序 按命令行方式存到txt中
4. 打开前n个文件
"""
import argparse
import os
import re


def find_files_with_mtime(log_directory, file_pattern):
    """Traverse directory,get all log files.

    :param log_directory: the directory on which we want to monitor
    :param file_pattern: regular expression of log file
    :return: all name matched files.
    """

    for each in log_directory:
        for root, dirs, files in os.walk(each):
            for one_file in files:
                if re.search(file_pattern, one_file):
                    abs_filename = root + os.sep + one_file
                    yield abs_filename, os.stat(abs_filename).st_mtime


class Finder(object):
    def __init__(self):
        parser = argparse.ArgumentParser(
            description='find N newest file under certain directory with ceratain pattern.')
        parser.add_argument('-n', metavar='return the newest N files ', type=str, nargs='*', dest='the_first_n',
                            help='path of your interesting image', required=True)

        parser.add_argument('-s', metavar='stor flag for newest log', type=str, nargs=1, dest='store_path',
                            help='path of directory you want to store', )
        self.namespace = parser.parse_args()


    def run(self):
        num, log_directory, pattern = self.namespace.the_first_n
        store_path = self.namespace.store_path
        # find file
        files = list(find_files_with_mtime([log_directory,], pattern))
        # sort it.
        if num >= len(files):
            self.sorted_files = sorted(files, key=lambda files: files[1], reverse=True)
        else:
            self.sorted_files = sorted(files, key=lambda files: files[1], reverse=True)[:int(num)]


        if store_path is not None:
            with open('newest_log.txt', 'w') as f:
                for one in self.sorted_files:
                    write_str = one[0] + '\n'
                    f.write(write_str)
        else:
            for one in self.sorted_files:
                print(one)
if __name__ == '__main__':
    finder = Finder()
    finder.run()






    # log_directory = ['D:\work\work_stuff\log\mts', ]
    # file_partten = '.*\.txt'
    # files = list(find_files_with_mtime(log_directory, file_partten))
    #
    # sorted_files = sorted(files, key=lambda files: files[1], reverse=True)
