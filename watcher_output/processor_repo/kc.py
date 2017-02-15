from logbook import Logger
import socket
import os
import pendulum
import requests
from influxdb import InfluxDBClient

log = Logger('k&C_post')


class InfluxDBBase(object):
    """ influxdb lib """

    def __init__(self, config):
        """ init """
        log.debug("init InfluxDBBase...")
        self.client = InfluxDBClient(config['host'],
                                     config['port'], config['username'],
                                     config['password'], config['db'])

    def send(self, json_body):
        """ write point """
        self.client.write_points(json_body, time_precision='u')


class Outputer(InfluxDBBase):
    def __init__(self, conf, processor):
        influxdb_conf = conf['influxdb']
        config_conf = conf['processor'][processor]['config']
        self.seq = 0
        self.HEADERS = {'content-type': 'application/json', 'accept': 'json', 'User-Agent': 'mabo'}
        self.hostname = socket.gethostname()
        self.nodename = config_conf["nodename"]
        self.eqpt_no = config_conf["eqpt_no"]
        self.status_set = conf['processor'][processor]['status']
        self.status_mapping = {'run': 1, 'stop': 0}
        super(Outputer, self).__init__(influxdb_conf)
        # TODO: sooner or later ,i will clean this complicated inition.

    def message_process(self, msg_line, task, measurement, inject_tags=None):
        # FIXME:the task is None teporaryily,used in construct_json
        self.seq += 1
        log_list = msg_line.split(' ', 5)  # separate message and time
        time_str = ''.join(log_list[:4])
        error_dict = dict()
        try:
            dt = pendulum.from_format(time_str, '%H:%M:%S%b%d%Y', 'Asia/Shanghai')
        except:
            error_dict['time'] = ('kc_50', time_str)
            return 1, error_dict
        time = dt.timestamp
        message = log_list[-1]
        # add status fields
        # this way make pop up message also get status true.

        status = self.get_status(message)
        script_name = Outputer.get_script_name(message)
        # task = self.get_task_name()
        # through this function,we can append extra info from the conf files.
        payload = self.construct_json(time, message, measurement, inject_tags,
                                      status, script_name, task=None)
        # print payload['data']['fields']['status']
        self.send([payload["data"]])
        log.debug('eps_kc sent !')
        return 0, None

    # def construct_json(self, time, message, item, inject_tags, status, task):
    def construct_json(self, *args, **kwargs):
        time, message, measurement, inject_tags, status, script_name = args
        task = kwargs['task']
        fields = {"Msg": message,
                  "status": status,
                  'script_name': script_name}

        tags = {"hostname": self.hostname,
                "node": self.nodename,
                "task": task,
                "seq": self.seq,
                "eqpt_no": self.eqpt_no, }
        if inject_tags is not None:
            tags.update(inject_tags)
        payload = {"data": {"tags": tags,
                            "fields": fields,
                            "time": 1000000 * int(time) + self.seq % 1000,
                            "measurement": measurement}}
        return payload

    def get_status(self, log_msg):
        status_set = self.status_set
        for one_set in status_set:
            if log_msg.startswith(tuple(status_set[one_set])):
                status = one_set
                return self.status_mapping[status]
        return None
        # reverse_set = {value: key for key, value in status_set.items()}

    @staticmethod
    def get_script_name(log_msg):
        if 'script' in log_msg:
            script_name = log_msg.split(' ', 5)[-1].split('/')[-1].split('.')[0]
            return script_name

