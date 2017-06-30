#!/usr/bin/env python
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

l = util.createlogger('HWPub', logging.INFO)


class ZKTest(HDaemonRepSrv):

    def __init__(self, port, run_data, zk_server_ip):
        self.run_data = run_data
        self.zk_server_ip = zk_server_ip
        HDaemonRepSrv.__init__(self, port)
        self.register_fn('startreader', self.start_reader)
        self.register_fn('startwriter', self.start_writer)
        self.register_fn('stop', self.stop)
        self.register_fn('getstats', self.get_stats)
        self.register_fn('waiting', self.waiting)

    def start_writer(self):
        self.run_data['test_action'] = 'startwriter'
        return 'ok', 'starting'

    def start_reader(self):
        self.run_data['test_action'] = 'startreader'
        return ('ok', 'starting')

    def waiting(self):
        return ('ok', self.run_data['test_status'])

    def stop(self):
        self.run_data['test_action'] = 'stop'
        return ('ok', 'stoppingclient')

    def get_stats(self):
        self.run_data['test_action'] = 'getstats'
        return ('ok', self.run_data['stats'])

    def writer(self):
        conn_start = time.time() * 1000
        zkw = KazooClient(hosts=self.zk_server_ip)
        zkw.start()
        conn_end = time.time() * 1000
        conn_diff = conn_end - conn_start
        l.info("About to start writing")
        totalwrite_end = 0
        write_time = []
        while True:
            if self.run_data['test_status'] == 'start':
                time.sleep(0.1)
                write_time_start = time.time() * 1000
                zkw.create("/Hydra/h-", b'', ephemeral=True, sequence=True)
                write_time_end = (time.time() * 1000)
                write_time_diff = write_time_end - write_time_start
                totalwrite_end = totalwrite_end + write_time_diff
                dict_write = {'write': {}, 'total': {}, 'conn': {}}
                dict_write['write'][write_time_end] = write_time_diff
                write_time.append(dict_write['write'])

            elif self.run_data['test_status'] == 'stop':
                l.info("Stopping writer connection")
                zkw.stop()
                dict_write['conn'][conn_end] = conn_diff
                dict_write['total'][time.time() * 1000] = totalwrite_end
                self.run_data['stats']['write_times'] = write_time
                l.info(self.run_data['stats'])
                self.run_data['test_status'] = 'stopped'
                break
        l.info("Writer test ended")
        return 'ok', None

    def reader(self):
        conn_start = time.time() * 1000
        zkr = KazooClient(hosts=self.zk_server_ip)
        zkr.start()
        conn_end = time.time() * 1000
        conn_diff = conn_end - conn_start
        l.info("Let's create arbitrary znodes for reading!")
        for nodes in range(5):
            zkr.ensure_path("/Hydra/Test/test-%s" % nodes, '')
        l.info("Done creating test znodes under /Hydra/Test/")
        l.info("About to start reading")
        totalread_end = 0
        read_time = []
        child = zkr.get_children("/Hydra/Test/")
        child_len = len(child)
        while True:
            if self.run_data['test_status'] == 'start':
                for r in child:
                    time.sleep(0.1)
                    l.info(r)
                    read_time_start = time.time() * 1000
                    data, stat = zkr.get("/Hydra/Test/%s" % r)
                    read_time_end = (time.time() * 1000)
                    read_time_diff = read_time_end - read_time_start
                    totalread_end = totalread_end + read_time_diff
                    dict_read = {'read': {}, 'total': {}, 'conn': {}}
                    dict_read['read'][read_time_end] = read_time_diff
                    read_time.append(dict_read['read'])
            elif self.run_data['test_status'] == 'stop':
                l.info("Stopping reader connection")
                zkr.stop()
                dict_read['conn'][conn_end] = conn_diff
                dict_read['total'][time.time() * 1000] = totalread_end
                self.run_data['stats']['read_times'] = read_time
                l.info(self.run_data['stats'])
                self.run_data['test_status'] = 'stopped'
                break
        l.info("Reader test ended")
        return 'ok', None


def run(argv):
    zk_server_ip = argv[1]
    max_time = int(argv[2])
    reader_rep_port = os.environ.get('PORT0')
    run_data = {'stats': {},
                'test_action': 'waiting',
                'test_status': 'stopped'}
    l.info("Starting ZKtest  at port [%s]", reader_rep_port)
    hd = ZKTest(reader_rep_port, run_data, zk_server_ip)
    hd.run()
    while True:
        l.info("Reader code running")
        if run_data['test_action'] == 'startreader':
            run_data['test_status'] = 'start'
            r = Thread(target=hd.reader)
            start_time = time.time()
            r.start()
            l.info(max_time)
            while (time.time() - start_time) < max_time:
                l.info(time.time() - start_time)
                time.sleep(0.1)
            l.info("time finished!")
            run_data['test_action'] = ''
            run_data['test_status'] = 'stop'

        elif run_data['test_action'] == 'startwriter':
            run_data['test_status'] = 'start'
            w = Thread(target=hd.writer)
            start = time.time()
            w.start()
            while (time.time() - start_time) < max_time:
                time.sleep(0.1)
            l.info("time finished")
            run_data['test_action'] = 'stop'

        elif run_data['test_action'] == 'getstats':
            time.sleep(2)
            break

        else:
            time.sleep(1)

if __name__ == "__main__":
    run(sys.argv)
