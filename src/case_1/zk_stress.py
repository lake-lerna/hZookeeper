#!/usr/bin/env python
import random
import json
import zmq
import os
import logging
import sys
import time
from kazoo.client import KazooClient
from sys import path
from threading import Thread
from hydra.lib import util
from hydra.lib.hdaemon import HDaemonRepSrv
path.append("hydra/src/main/python")

l = util.createlogger('ZKStress', logging.INFO)


class ZKStress(HDaemonRepSrv):

    def __init__(self, port, run_data, zk_server_ip):
        self.run_data = run_data
        self.zk_server_ip = zk_server_ip
        HDaemonRepSrv.__init__(self, port)
        self.register_fn('startwriter', self.stress_writer)
        self.register_fn('stopstress', self.stress_stop)
        self.register_fn('startreader', self.stress_reader)
        self.register_fn('no', self.do_nothing)

    def do_nothing(self):
        return 'ok', 'nothing'

    def stress_writer(self):
        self.run_data['test_action'] = 'startwriter'
        return 'ok', self.run_data['test_action']

    def stress_stop(self):
        self.run_data['test_action'] = 'stopstress'
        return ('ok', 'stress_stop')

    def stress_reader(self):
        self.run_data['test_action'] = 'startreader'
        return ('ok', 'startreader')

    def reader(self, j):
        l.info("Thread-%s" % j)
        zk = KazooClient(hosts=self.zk_server_ip)
        zk.start()
        while True:
            if self.run_data['test_action'] == 'stopstress':
                l.info("Stopping thread-%s" % j)
                zk.stop()
                break
            else:
                for i in range(1000):
                    data, stat = zk.get("/Hydra/Test/test-0")
        return 'ok', None

    def write(self, j):
        l.info("Thread-%s" % j)
        znodes = []
        while True:
            if self.run_data['test_action'] == 'stopstress':
                l.info("Stopping thread-%s" % j)
                break
            else:
                conn_start = time.time() * 1000
                # Connection to the zookeeper server
                zk = KazooClient(hosts=self.zk_server_ip)
                zk.start()
                conn_end = time.time() * 1000
                conn_diff = conn_end - conn_start
                for i in range(1000):
                    t = zk.create(
                        "/Hydra/h-",
                        b'Muneeb',
                        ephemeral=True,
                        sequence=True)
                zk.stop()
            time.sleep(0.5)
        return 'ok', None


def run(argv):
    """
    This function would be called when zk_stress app will be launched.
    :param argv: Function will take publisher_port as argument. A ZMQ publisher socket will be opened with this port.
    :return:
    """
# Use PORT0 (this is the port which Mesos assigns to the applicaiton), as control port. HAnalyzer will send all
# signals to this port.
    zk_server_ip = argv[1]
    list_threads = []
    stress_rep_port = os.environ.get('PORT0')
    run_data = {'test_action': 'waiting',
                'test_status': 'stopped'}
    print ("Starting ZKstress  at port [%s]", stress_rep_port)
    hd = ZKStress(stress_rep_port, run_data, zk_server_ip)
    hd.run()
    j = 1  # initialize thread count
    while True:
        if run_data['test_action'] == 'startwriter':
            l.info("Starting writer stress threads")
            run_data['test_status'] = 'start'
            for t in range(5):
                r = Thread(target=hd.write, args=(j,))
                r.start()
                j += 1
            run_data['test_action'] = 'waiting'
        elif run_data['test_action'] == 'startreader':
            l.info("Starting reader stress threads")
            for t in range(5):
                w = Thread(target=hd.reader, args=(j,))
                w.start()
                j += 1
            run_data['test_action'] = 'waiting'

        elif run_data['test_action'] == 'stopstress':
            l.info("Got stopstress signal")
            run_data['test_status'] = 'stop'
            break
        else:
            l.info("waiting for a signal to start stressing")
            time.sleep(2)

if __name__ == "__main__":
    run(sys.argv)
