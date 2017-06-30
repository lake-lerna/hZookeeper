#!/usr/bin/env python
import numpy
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
path.append("hydra/src/main/python")
from hydra.lib.runtestbase import HydraBase
from ConfigParser import ConfigParser
from optparse import OptionParser
from hydra.lib.h_analyser import HAnalyser
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
            test_name='ZKstress',
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

    def run_test(self):
        """
        Function which actually runs
        """
        self.start_init()
        self.launch_zk_client()
        self.launch_zk_stress(self.options)
        self.influxdb_reset()
        self.post_run(self.options)

    def post_run(self, options):
        print "Starting test"
        self.options = options
        while True:
            if self.options.stress_type != 'no signal':
                task_list = self.all_task_ids[self.zk_stress_app_id]
                for task_id in task_list:
                    info = self.apps[
                        self.zk_stress_app_id]['ip_port_map'][task_id]
                    port = info[0]
                    ip = info[1]
                    self.zk_an = ZKAnalyzer(ip, port, task_id)
                    # Sending a signal towards stress client and waiting for it's response
                    (status, resp) = self.zk_an.do_req_resp(
                        self.options.stress_type, tout_60s)
                self.options.stress_type = 'no signal'
            else:
                print "no stress signal yet"

            if self.options.test_type != 'no signal':
                # Getting task list which in this case is only one task
                task_list = self.all_task_ids[self.zk_client_app_id]
                for task_id in task_list:
                    info = self.apps[
                        self.zk_client_app_id]['ip_port_map'][task_id]
                    port = info[0]
                    ip = info[1]
                    self.zk_an = ZKAnalyzer(ip, port, task_id)
                    # Sending a signal towards test client and waiting for it's response
                    (status, resp) = self.zk_an.do_req_resp(
                        self.options.test_type, tout_60s)
                    print "Test started which would run for %ssec" % self.options.run_time
                # Set the signal to 'waiting' so we can wait for test to end before fetching stats 
                self.options.test_type = 'waiting'

            else:
                print "no test signal"

            if self.options.test_type == 'waiting':
                print "Wait for a moment....."

                while True:
                    time.sleep(2)
                    for task_id in task_list:
                        info = self.apps[
                            self.zk_client_app_id]['ip_port_map'][task_id]
                        port = info[0]
                        ip = info[1]
                        self.zk_an = ZKAnalyzer(ip, port, task_id)
                        # Waiting for test to end
                        (status, resp) = self.zk_an.do_req_resp(
                            self.options.test_type, tout_60s)
                    # if the response is 'stopped', we can fetch the stats now
                    if resp == 'stopped':
                        self.options.test_type = 'getstats'
                        self.zk_an = ZKAnalyzer(ip, port, task_id)
                        (status, resp) = self.zk_an.do_req_resp(
                            self.options.test_type, tout_60s)
                        # Parse the results and store them in influxdb
                        self.results(resp)
                        self.options.test_type = 'done'
                        break
            if self.options.test_type == 'done':
                print "Done with the test"
                self.options.test_type = 'no signal'
                break

            else:
                print "no test signal"
                time.sleep(1)
            time.sleep(1)

    def results(self, resp):
        for stats_key in resp.keys():
            print "************"
            dict = {stats_key: {}}
            list = resp[stats_key].strip('[]').split(',')
            for d in list:
                dict[stats_key][
                    float(
                        d.strip().strip('{}').split(":")[0])] = float(
                    d.strip().strip('{}').split(":")[1])
            perc = numpy.percentile(dict[stats_key].values(), 95)
            med = numpy.median(dict[stats_key].values())
            mean = numpy.mean(dict[stats_key].values())
            print "***** STATS *****"
            print "Mean : %s ms" % mean
            print "Median : %s ms" % med
            print "95 Percentile : %s ms" % perc
            print "*****************"

    def influxdb(self, json_body):
        client = InfluxDBClient(
            host='10.10.0.88',
            port=8086,
            username='root',
            password='root',
            database='hZookeeper')
        dbs = client.get_list_database()
        # check if the db already exists
        try:
            t = dbs[1]
        except:
            client.create_database('hZookeeper')
        client.write_points(json_body)
        print "done writing data"

    def influxdb_reset(self):
        client = InfluxDBClient(
            host='10.10.0.88',
            port=8086,
            username='root',
            password='root',
            database='hZookeeper')
        dbs = client.get_list_database()
        print "About to reset db..."
        client.drop_database('hZookeeper')
        print "Done Resetting!"

    def launch_zk_client(self):
        print "Launching ZK test client app"
        print self.options.zk_server_ip
        threads_per_client = 5
        self.create_binary_app(
            name=self.zk_client_app_id,
            app_script='./src/case_3/zk_client.py %s %s' %
            (self.options.zk_server_ip,
             self.options.run_time),
            cpus=0.1,
            mem=128,
            ports=[0])

    def launch_zk_stress(self, options):
        """
        Function to launch zookeeper stress app.
        """
        print ("Launching the Zookeeper stress app")
        self.options = options
        max_threads = 10
        stress_clients = self.options.stress_clients/max_threads
        self.create_binary_app(
            name=self.zk_stress_app_id,
            app_script='./src/case_3/zk_stress.py  %s %s' %
            (self.options.zk_server_ip,
             max_threads),
            cpus=0.05,
            mem=100,
            ports=[0])
        print "Stress App Started Succesfully"
        print "Scaling app: %s to count: %s ..." % (self.zk_stress_app_id, stress_clients)
        self.scale_and_verify_app(
            self.zk_stress_app_id,
            stress_clients)
        print ("Successfully Scaled!")


class RunTest(object):

    def __init__(self, argv):
        usage = ('python %prog --zk_server_ip=<ip:port> --test_type=<reader> --stress_type=<writer> --run_time=<50>')

        parser = OptionParser(description='zookeeper scale test master',
                              version="0.1", usage=usage)
        parser.add_option(
            "--zk_server_ip",
            "-z",
            dest='zk_server_ip',
            help="Zookeeper Cluster ip/ips",
            default='10.10.4.244:2181',
            type='str')
        parser.add_option(
            "--test_type",
            "-t",
            dest='test_type',
            help="Test type, Read/Write",
            default='startreader',
            type='str')
        parser.add_option(
            "--stress_type",
            "-s",
            dest='stress_type',
            help="Stress type, Read/Write",
            default='startreader',
            type='str')
        parser.add_option(
            "--stress_clients",
            "-c",
            dest='stress_clients',
            help="Number of stress clients, Number should be multiple of 10",
            default=1,
            type='int')
#        parser.add_option(
#            "--threads_per_client",
#            "-p",
#            dest='threads_per_client',
#            help="Threads per stress client",
#            default=5,
#            type='int')
        parser.add_option(
            "--run_time",
            "-r",
            dest='run_time',
            default=120,
            help="Test duration in seconds",
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
