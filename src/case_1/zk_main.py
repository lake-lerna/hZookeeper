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
path.append("hydra/src/main/python")
from hydra.lib.runtestbase import HydraBase
from ConfigParser import ConfigParser
from optparse import OptionParser
from hydra.lib.h_analyser import HAnalyser
tout_60s = 60000



class ZKPubAnalyser(HAnalyser):
	def __init__(self, server_ip, server_port, task_id):
		HAnalyser.__init__(self, server_ip, server_port, task_id)
		

class ZK(HydraBase):
	def __init__(self, options):
		self.config = ConfigParser()
		self.options = options
		HydraBase.__init__(self, test_name='ZKstress', options=self.options, app_dirs=['src', 'hydra'])
		self.zk_client_app_id = self.format_appname("/zk-test")
		self.zk_stress_app_id = self.format_appname("/zk-stress")
		self.zk_client_task_ip = None
		self.zk_stress_task_ip = None
		self.zk_client_cmd_port = None
		self.zk_stress_cmd_port = None
		self.zk_an = None  # Stress Analyzer
		self.zkra = None	# Reader Analyzer		
		self.add_appid(self.zk_stress_app_id)
		self.add_appid(self.zk_client_app_id)
		self.threads_count = {}
#		self.options = {'test_type':'no signal', 'stress_type':'no signal', 
#						'stress_status': 'stopped', 'reader_status': 'stopped', 
#						'self.threads_count': {}, 'threads_per_client': 0, 'stress_clients': 0}		

	def run_test(self):
		"""
		Function which actually runs
		"""
		self.start_init()
#		self.input()
		self.launch_zk_client()
		self.launch_zk_stress(self.options)
		self.influxdb_reset()
		self.post_run(self.options, self.options)
#		self.influxdb()
	
#	def input(self):
#		if self.options.test_type == 'no signal': 
#		self.options.test_type = raw_input('Enter the desired test action: ')
#		self.options.stress_type = raw_input('Enter the desired stress action: ')
#		self.options['stress_clients'] = int(raw_input('Enter the desired stress clients: '))
#		self.options.threads_per_client = int(raw_input('Enter the desired stress threads/client: '))
			
#		print "Processing inputs!...."
#		else:
#			pass				
			
	def signals_an(self,app_id, test_type, test_info, tout_60s):
			
		self.options = test_info
		print app_id, test_type

		if app_id == self.zk_client_app_id:
#		for task_id in task_list:
#			info = self.apps[app_id]['ip_port_map'][task_id]	
#			port = info[0]
#			ip = info[1]			
						
			task_id = self.apps[self.zk_client_app_id]['ip_port_map'].keys()
			info = self.apps[self.zk_client_app_id]['ip_port_map'].values()
			port = info[0][0]
			ip = info[0][1]
			print ("Reader is running on %s : %s" %(ip, port))

		if app_id == self.zk_stress_app_id:
			task_id = min(self.threads_count, key=self.threads_count.get)
			threads_count = self.threads_count[task_id]
			print threads_count, self.options.threads_per_client
			info = self.apps[self.zk_stress_app_id]['ip_port_map'][task_id]
			port = info[0]
			ip = info[1]				
			if  threads_count == self.options.threads_per_client:
				
#				if self.options.test_type=='startstress':
#				self.threads_count = dict.fromkeys(self.threads_count, 0)
				self.options.test_type='stop'
				self.options.stress_type='no signal'
				self.zk_an = ZKPubAnalyser(ip, port, task_id)
				print "Sending stop signal to stress client"
				(status, resp) = self.zk_an.do_req_resp('stopstress', tout_60s)
							
				return None, None
#		for task_id in task_list:
#			info = self.apps[app_id]['ip_port_map'][task_id]	
#			port = info[0]
#			ip = info[1]
		self.zk_an = ZKPubAnalyser(ip, port, task_id)
		print "Sending %s to task_id: %s " %(test_type,task_id)		
		(status, resp) = self.zk_an.do_req_resp(test_type, tout_60s)
		print "Done sending and got the resp: %s"%resp
		print status, resp
		if app_id == self.zk_stress_app_id:
			self.threads_count[task_id] += 5
		return status, resp
					
			
	def post_run(self, test_type, options):
#		self.options = test_info
		self.options = options
		self.started_stress_task_list = []
#		info_reader = self.apps[self.zk_client_app_id]['ip_port_map'].values()

#		print ("Reader is running on %s : %s" %(reader_ip, reader_port))

		#########*****
		stress_task_list = self.all_task_ids[self.zk_stress_app_id]
		for task_id in stress_task_list:
			self.threads_count[task_id] = 0
			print "initializing threads count"
			print self.threads_count
#			info_stress = self.apps[self.zk_stress_app_id]['ip_port_map'][stress_task_list[0]]
#			iport = info_stress[0]
#			iip = info_stress[1]
#			self.zk_an = ZKPubAnalyser(iip, iport, task_id)
#			print "sending signal"
#			(status, resp) = self.zk_an.do_req_resp('startstress', tout_60s)		
#			print "done signal"
		first_time = True
		while True:
#			self.input()
#			time.sleep(2)
			if self.options.stress_type != 'no signal':
				while True:
					time.sleep(2)
					
					(status, resp) = self.signals_an(self.zk_stress_app_id, self.options.stress_type, self.options, tout_60s)
					print resp
					if first_time == True:
						first_time = False
						break
					if resp == None:
						self.options.stress_type = 'no signal'
						self.options.test_type = 'stop'
						break
										
#				self.threads_count[min_stress_task] += 5
				#self.options.test_type = 'teststart'
				
			else:
				print "no stress signal"
#				if first_time == True:
#					first_time = False
#				else:
#					self.options.test_type = 'stop'
#				time.sleep(15)
						
			if self.options.test_type != 'no signal':
				(status, resp) = self.signals_an(self.zk_client_app_id, self.options.test_type, self.options, tout_60s)
				if self.options.test_type == 'getstats':

					print "Getting stats"
					self.results(resp)
					break

				if self.options.test_type == 'stop':
					self.options.test_type = 'waiting'
					while True:
						time.sleep(2)
						(status, resp) = self.signals_an(self.zk_client_app_id, self.options.test_type, self.options, tout_60s)
						if resp == 'stopped':
							self.options.test_type = 'getstats'
							break
			else:
				print "no test signal"
				pass			

	def results(self, resp):
		for stats_key in resp.keys():
			print "************"
			dict={stats_key:{}}
			list=resp[stats_key].strip('[]').split(',')
			for d in list:
				
				dict[stats_key][float(d.strip().strip('{}').split(":")[0])] = float(d.strip().strip('{}').split(":")[1])
			print dict
			json_body = [{"measurement" : "hZookeeper_stats", "tags":{}, "time":'', "fields" : {}}]
			c=0
			for k in dict[stats_key].keys():
				c+=1
				json_body[0]["tags"]['count']=c
				time_db = datetime.datetime.fromtimestamp(float(k)/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
				json_body[0]["time"] = time_db
				json_body[0]["fields"][stats_key] = float(dict[stats_key][k])
				print json_body
				self.influxdb(json_body)


	def influxdb(self, json_body):
		client = InfluxDBClient(host='10.10.0.88', port=8086, username='root', password='root', database='hZookeeper')
		dbs = client.get_list_database()
#		print dbs
		try:
			t=dbs[1]
		except:
#			print "NO DB"
			client.create_database('hZookeeper')
#		print type(json_body)
		client.write_points(json_body)
		print "done writing data"
#		client.drop_database('hZookeeper')		

	def influxdb_reset(self):
        	client = InfluxDBClient(host='10.10.0.88', port=8086, username='root', password='root', database='hZookeeper')
                dbs = client.get_list_database()
                print "About to reset db"
		client.drop_database('hZookeeper')
		print "Done resetting"
	def launch_zk_client(self):
		print "Launching reader app"
		print self.options.zk_server_ip
		threads_per_client = 5
		self.create_binary_app(name=self.zk_client_app_id, app_script='./src/case_1/zk_client.py %s'
									% (self.options.zk_server_ip),	
					cpus=0.1, mem=128, ports=[0])
	
		
	def launch_zk_stress(self, options):
		"""
		Function to launch zookeeper stress app.
		"""
		print ("Launching the Zookeeper stress app")
		self.options = options
#		max_threads_per_client = 5
#		print type(self.options)
		self.create_binary_app(name=self.zk_stress_app_id, app_script='./src/case_1/zk_stress_write.py  %s'
										 % self.options.zk_server_ip,
	                               cpus=0.1, mem=128, ports=[0])
		print "Stress app started succesffully"
		print "Scaling app: %s to count: %s" %(self.zk_stress_app_id, self.options.stress_clients)
		self.scale_and_verify_app(self.zk_stress_app_id, self.options.stress_clients)
		print ("Successfully scaled")
		

#		time.sleep(20)
class RunTest(object):
	def __init__(self, argv):
        	usage = ('python %prog --zk_server_ip=<ip:port> --test_type=startreader'
        				'--stress_type=startreader --stress_clients=1 --threads_per_client=5')

        	parser = OptionParser(description='zookeeper scale test master',
        	                      version="0.1", usage=usage)
		parser.add_option("--zk_server_ip", dest='zk_server_ip', default='10.10.4.244:2181', type='str')
		parser.add_option("--test_type", dest='test_type', default='startreader', type='str')
		parser.add_option("--stress_type", dest='stress_type', default='startreader', type='str')
		parser.add_option("--stress_clients", dest='stress_clients', default=1, type='int')
		parser.add_option("--threads_per_client", dest='threads_per_client', default=5, type='int')
#		parser.add_option("--stress_reader", dest='stress_reader', default='no', type='str')
		(options, args) = parser.parse_args()
		if ((len(args) != 0)):
			parser.print_help()
			sys.exit(1)


		
		print options

		r = ZK(options)

		r.start_appserver()

		r.run_test()


#	        print ("About to sleep for 15")
#       time.sleep(15)
#		r.delete_all_launched_apps()
		r.stop_appserver()

if __name__ == "__main__":
	RunTest(sys.argv)

