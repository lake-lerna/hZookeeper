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

path.append("hydra/src/main/python")

from hydra.lib import util
from hydra.lib.hdaemon import HDaemonRepSrv

l = util.createlogger('HWPub', logging.INFO)


class ZKPub(HDaemonRepSrv):
	def __init__(self,port,run_data,zk_server_ip):
		self.run_data = run_data
		self.zk_server_ip = zk_server_ip
		HDaemonRepSrv.__init__(self,port)
		self.register_fn('startreader', self.start_reader)
		self.register_fn('startwriter', self.start_writer)
		self.register_fn('stop', self.stop)
		self.register_fn('getstats', self.get_stats)
		self.register_fn('waiting', self.waiting)
	def start_writer(self):
		self.run_data['test_action']='startwriter'
		return 'ok', 'starting'

#	def stop_writer(self):
#		self.run_data['test_action']='stopwriter'
#		return 'ok', 'stopping'
	def start_reader(self):
		self.run_data['test_action']='startreader'
		return ('ok', 'starting')

	def waiting(self):
		return ('ok', self.run_data['test_status'])
	
	def stop(self):
		self.run_data['test_action']='stop'
		return ('ok', 'stoppingclient')

	def get_stats(self):
		self.run_data['test_action']='getstats'
#		stats = json.dumps(self.run_data['stats'])
		return ('ok', self.run_data['stats'])
	def writer(self):
                conn_start = time.time()*1000
                zkw = KazooClient(hosts=self.zk_server_ip)
                zkw.start()
                conn_end = time.time()*1000
                conn_diff = conn_end - conn_start
                l.info("About to start writing")
                totalwrite_end = 0
                write_time = []
                while True:
#                       time.sleep(0.5)
                        if self.run_data['test_status'] == 'start':
				time.sleep(0.1)
				write_time_start=time.time()*1000
				zkw.create("/Hydra/h-", b'', ephemeral=True, sequence=True)
				write_time_end=(time.time()*1000)
				write_time_diff = write_time_end - write_time_start				
				totalwrite_end = totalwrite_end + write_time_diff	
				dict_write={'write':{}, 'total':{}, 'conn':{}}
				dict_write['write'][write_time_end] = write_time_diff
				write_time.append(dict_write['write'])		

                        elif self.run_data['test_status'] == 'stop':
                                l.info("Stopping writer connection")
                                zkw.stop()
				dict_write['conn'][conn_end] = conn_diff
				dict_write['total'][time.time()*1000] = totalwrite_end
                                self.run_data['stats']['write_times'] = write_time
                                self.run_data['stats']['total_writes'] = dict_write['total']
                                self.run_data['stats']['connection'] = dict_write['conn']
                                l.info(self.run_data['stats'])   
#                               self.run_data['test_action'] = 'waiting'
                                self.run_data['test_status'] = 'stopped'
                                break				
		l.info("Writer test ended")
		return 'ok', None
	def reader(self, j):
		l.info("started thread-%s"%j)
		conn_start = time.time()*1000
		zkr = KazooClient(hosts=self.zk_server_ip)
		zkr.start()
		conn_end = time.time()*1000
		conn_diff = conn_end - conn_start
		l.info("Let's create arbitrary znodes for reading!")
		for nodes in range(5):
			zkr.ensure_path("/Hydra/Test/test-%s"%nodes, '')
		l.info("Done creating test znodes under /Hydra/Test/")
		l.info("About to start reading")
		totalread_end = 0
		read_time = []
		child = zkr.get_children("/Hydra/Test/")
		child_len = len(child)
		while True:
#			time.sleep(0.5)
			if self.run_data['test_status'] == 'start':
				for r in child:
					time.sleep(0.1)
					l.info(r)
					read_time_start=time.time()*1000
					data, stat = zkr.get("/Hydra/Test/%s"%r)
					read_time_end=(time.time()*1000)
					read_time_diff = read_time_end - read_time_start
#					l.info(read_time_diff)
					totalread_end = totalread_end + read_time_diff
#					l.info(totalread_end)
					dict_read={'read':{}, 'total':{}, 'conn':{}}
					dict_read['read'][read_time_end] = read_time_diff
#					l.info(dict_read)
					read_time.append(dict_read['read'])
			elif self.run_data['test_status'] == 'stop':
				l.info("Stopping reader connection")
				zkr.stop()
				dict_read['conn'][conn_end] = conn_diff
				dict_read['total'][time.time()*1000] = totalread_end
				self.run_data['stats']['read_times'] = read_time
				self.run_data['stats']['total_reads'] = dict_read['total']
				self.run_data['stats']['connection'] = dict_read['conn']
				l.info(self.run_data['stats'])
#				self.run_data['test_action'] = 'waiting'
				self.run_data['test_status'] = 'stopped'
				
				break										
		l.info("Reader test ended")
#			if self.run_data['test_status'] == 'stop':
#				zkr.stop()
#				l.info("Reader test ended")					
#				break
		return 'ok', None

def run(argv):
	zk_server_ip=argv[1]
	reader_rep_port = os.environ.get('PORT0')
	run_data = {'stats': {},
		    'test_action': 'waiting',
		    'test_status': 'stopped'}
	l.info ("Starting ZKreader  at port [%s]", reader_rep_port)		    	
	hd = ZKPub(reader_rep_port, run_data, zk_server_ip)
	hd.run()
	j = 1
	while True:
		l.info("Reader code running")
		if run_data['test_action'] == 'startreader':
			run_data['test_status'] = 'start'
			l.info("Starting reader thread-%s"%j)
			r = Thread(target=hd.reader , args=(j,))
			r.start()
			
			run_data['test_action'] = 'waiting'
			j += 1
		elif run_data['test_action'] == 'startwriter':
			run_data['test_status'] = 'start'
			l.info("Starting writer thread-%s"%j)
			w = Thread(target=hd.writer)
			w.start()
			
			run_data['test_action'] = 'waiting'
			j += 1
			
		elif run_data['test_action'] == 'stop':
			l.info("stopping test")
			run_data['test_action'] = 'waiting'
			run_data['test_status'] = 'stop'
			time.sleep(1)
#			break
		elif run_data['test_action'] == 'getstats':
			time.sleep(2)
			break
		else:
			time.sleep(1)	

	
	
if __name__ == "__main__":
	run(sys.argv)	
	
	
	
	
	
	
	
