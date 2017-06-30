#!/usr/bin/env python
from threading import Thread
from ast import literal_eval
import re
import json
from influxdb import InfluxDBClient
import datetime
import time
import sys
import math
from sys import path
from hydra.lib.runtestbase import HydraBase
from ConfigParser import ConfigParser
from optparse import OptionParser
from hydra.lib.h_analyser import HAnalyser
path.append("hydra/src/main/python")
tout_60s = 60000


class ZKAnalyzer(HAnalyser):

    def __init__(self, server_ip, server_port, task_id):
        HAnalyser.__init__(self, server_ip, server_port, task_id)


class ZK(HydraBase):

    def __init__(self, options):
        self.config = ConfigParser()
        self.options = options
        HydraBase.__init__(
            self,
            test_name='ZKStress',
            options=self.options,
            app_dirs=[
                'src',
                'hydra'])
        self.zk_client_app_id = self.format_appname("/zk-test")
        self.zk_stress_app_id = self.format_appname("/zk-stress")
        self.zk_client_task_ip = None
        self.zk_stress_task_ip = None
        self.zk_client_cmd_port = None
        self.zk_stress_cmd_port = None
        self.zk_an = None  # Analyzer
        self.add_appid(self.zk_stress_app_id)
        self.add_appid(self.zk_client_app_id)
        self.threads_count = {}

    def run_test(self):
        """
        Function which actually runs
        """
        self.start_init()
        self.launch_zk_client()
        self.launch_zk_stress(self.options)
        self.influxdb_reset()
        self.post_run(self.options, self.options)

    def signals_an(self, app_id, test_type, test_info, tout_60s):
        """
        Function that would be used to send HAnalyzer signals
        """
        self.options = test_info
        print app_id, test_type
        if app_id == self.zk_client_app_id:
            task_id = self.apps[self.zk_client_app_id]['ip_port_map'].keys()
            info = self.apps[self.zk_client_app_id]['ip_port_map'].values()
            port = info[0][0]
            ip = info[0][1]
            print ("Reader is running on %s : %s" % (ip, port))

        if app_id == self.zk_stress_app_id:
            task_id = min(self.threads_count, key=self.threads_count.get)
            threads_count = self.threads_count[task_id]
            print threads_count, self.options.threads_per_client
            info = self.apps[self.zk_stress_app_id]['ip_port_map'][task_id]
            port = info[0]
            ip = info[1]
            if threads_count == self.options.threads_per_client:
                self.options.test_type = 'stop'
                self.options.stress_type = 'no signal'
                self.zk_an = ZKAnalyzer(ip, port, task_id)
                print "Sending stop signal to stress client"
                (status, resp) = self.zk_an.do_req_resp('stopstress', tout_60s)
                return None, None
        self.zk_an = ZKAnalyzer(ip, port, task_id)
        print "Sending %s to task_id: %s " % (test_type, task_id)
        (status, resp) = self.zk_an.do_req_resp(test_type, tout_60s)
        print "Done sending and got the resp: %s" % resp
        print status, resp
        if app_id == self.zk_stress_app_id:
            self.threads_count[task_id] += 5
        return status, resp

    def post_run(self, test_type, options):
        """
        Function where we would send signals towards test and stress app
        """
        self.options = options
        self.started_stress_task_list = []
        stress_task_list = self.all_task_ids[self.zk_stress_app_id]
        for task_id in stress_task_list:
            self.threads_count[task_id] = 0
            print "initializing threads count"
            print self.threads_count
        first_time = True
        while True:
            if self.options.stress_type != 'no signal':
                while True:
                    time.sleep(2)
                    (status, resp) = self.signals_an(self.zk_stress_app_id,
                                                     self.options.stress_type, self.options, tout_60s)
                    print resp
                    if first_time:
                        first_time = False
                        break
                    if resp is None:
                        self.options.stress_type = 'no signal'
                        self.options.test_type = 'stop'
                        break
            else:
                print "no stress signal"
            if self.options.test_type != 'no signal':
                (status, resp) = self.signals_an(self.zk_client_app_id,
                                                 self.options.test_type, self.options, tout_60s)
                if self.options.test_type == 'getstats':
                    print "Getting stats"
                    self.results(resp)
                    break
                if self.options.test_type == 'stop':
                    self.options.test_type = 'waiting'
                    while True:
                        time.sleep(2)
                        (status, resp) = self.signals_an(self.zk_client_app_id,
                                                         self.options.test_type, self.options, tout_60s)
                        if resp == 'stopped':
                            self.options.test_type = 'getstats'
                            break
            else:
                print "no test signal"
                pass

    def results(self, resp):
        """
        Function where the string of stats received would be split up into the actual data and
        stored in the influxdb later
        """
        for stats_key in resp.keys():
            print "************"
            dict = {stats_key: {}}
            list = resp[stats_key].strip('[]').split(',')
            for d in list:
                dict[stats_key][
                    float(
                        d.strip().strip('{}').split(":")[0])] = float(
                    d.strip().strip('{}').split(":")[1])
            json_body = [{"measurement": "hZookeeper_stats",
                          "tags": {}, "time": '', "fields": {}}]
            c = 0
            print "Storing results in InfluxDB..."
            for k in dict[stats_key].keys():
                c += 1
                json_body[0]["tags"]['count'] = c
                time_db = datetime.datetime.fromtimestamp(
                    float(k) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
                json_body[0]["time"] = time_db
                json_body[0]["fields"][stats_key] = float(dict[stats_key][k])
                self.influxdb(json_body)

    def influxdb(self, json_body):
        """
        Function that stores data in Influxdb
        replace the ip and port according to your influxdb config
        """
        client = InfluxDBClient(
            host='10.10.0.88',
            port=8086,
            username='root',
            password='root',
            database='hZookeeper')
        dbs = client.get_list_database()
        try:
            t = dbs[1]
        except:
            client.create_database('hZookeeper')
        client.write_points(json_body)
        print "Done writing data to InfluxDB!"

    def influxdb_reset(self):
        """
        Function to flush the data stored from previous test
        """
        client = InfluxDBClient(
            host='10.10.0.88',
            port=8086,
            username='root',
            password='root',
            database='hZookeeper')
        dbs = client.get_list_database()
        print "About to Reset DB"
        client.drop_database('hZookeeper')
        print "Done Resetting"

    def launch_zk_client(self):
        """
        Function to launch the zookeeper test client app
        """
        print "Launching reader app"
        print self.options.zk_server_ip
        threads_per_client = 5
        self.create_binary_app(
            name=self.zk_client_app_id,
            app_script='./src/case_1/zk_client.py %s' %
            (self.options.zk_server_ip),
            cpus=0.1,
            mem=128,
            ports=[0])

    def launch_zk_stress(self, options):
        """
        Function to launch zookeeper stress app.
        """
        print ("Launching the Zookeeper stress app")
        self.options = options
        self.create_binary_app(
            name=self.zk_stress_app_id,
            app_script='./src/case_1/zk_stress.py  %s' %
            self.options.zk_server_ip,
            cpus=0.1,
            mem=128,
            ports=[0])
        print "Stress app started succesffully"
        print "Scaling app: %s to count: %s" % (self.zk_stress_app_id, self.options.stress_clients)
        self.scale_and_verify_app(
            self.zk_stress_app_id,
            self.options.stress_clients)
        print ("Successfully scaled")


class RunTest(object):

    def __init__(self, argv):
        usage = (
            'python %prog --zk_server_ip=<ip:port> --test_type=startreader'
            '--stress_type=startreader --stress_clients=1 --threads_per_client=5')
        parser = OptionParser(description='zookeeper scale test master',
                              version="0.1", usage=usage)
        parser.add_option(
            "--zk_server_ip",
            dest='zk_server_ip',
            default='10.10.4.244:2181',
            type='str')
        parser.add_option(
            "--test_type",
            dest='test_type',
            default='startreader',
            type='str')
        parser.add_option(
            "--stress_type",
            dest='stress_type',
            default='startreader',
            type='str')
        parser.add_option(
            "--stress_clients",
            dest='stress_clients',
            default=1,
            type='int')
        parser.add_option(
            "--threads_per_client",
            dest='threads_per_client',
            default=5,
            type='int')
        (options, args) = parser.parse_args()
        if ((len(args) != 0)):
            parser.print_help()
            sys.exit(1)
        print options
        r = ZK(options)
        r.start_appserver()
        r.run_test()
        r.delete_all_launched_apps()
        r.stop_appserver()
        
if __name__ == "__main__":
    RunTest(sys.argv)
