#!/usr/bin/env python
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
		self.zk_pub_app_id = self.format_appname("/zk-pub")
		self.zk_pub_task_ip = None
		self.zk_pub_cmd_port = None
		self.zkpa = None  # Pub Analyzer
		self.add_appid(self.zk_pub_app_id)

	def run_test(self):
		"""
		Function which actually runs
		"""
		self.start_init()
		self.launch_zk_pub()
		self.post_run(self.options)
#		self.influxdb()
		
	def post_run(self,options):
		self.options = options
							
		task_list = self.all_task_ids[self.zk_pub_app_id]
		print task_list
		print self.zk_pub_app_id
		print self.apps[self.zk_pub_app_id]['ip_port_map'].values()[0][0]
		print self.apps[self.zk_pub_app_id]['ip_port_map']
		print ("Communicating signals to zk_stress_client")

		for task_id in task_list:
			info = self.apps[self.zk_pub_app_id]['ip_port_map'][task_id]
			port = info[0]
			ip = info[1]
			ha_list = []
			self.zkpa = ZKPubAnalyser(ip, port, task_id)
			print "Sending sengmsg signal to %s : %s" %(ip,port)
			self.zkpa.do_req_resp('sendmsg', tout_60s)
			ha_list.append(self.zkpa)

		id = 0
		app_id = 1
		for task_id in task_list:
#			print task_id
			info = self.apps[self.zk_pub_app_id]['ip_port_map'][task_id]
			port = info[0]
			ip = info[1]
			self.zkpa = ZKPubAnalyser(ip, port, task_id)
			print "*****************"
			print "Sending teststatus signal to %s : %s" %(ip,port)			
			
			while True:
				(stat, r)=self.zkpa.do_req_resp('teststatus', tout_60s)
				if r=='stopping':
					break
				time.sleep(1)
			print "Done waiting"
			print "Getting stats" 
			(status, resp) = self.zkpa.do_req_resp('getstats', tout_60s)
			print resp
	
			for stats_key in resp.keys():
				if stats_key == 'watches':
					dict = {stats_key:{}}
					list=resp[stats_key].strip('[]').split(',')
					for d in list:
						dict[stats_key][float(d.strip().strip('{}').split(":")[0])] = float(d.strip().strip('{}').split(":")[1])
					print "********"
					print "YOOOOO----%s"%dict
					for w in dict['watches'].keys():
						json_body = [{"measurement" : "hZookeeper_stats", "tags":{}, "time":'', "fields" : {}}]
						json_body[0]["tags"]['app'] = app_id
						time_db = datetime.datetime.fromtimestamp(float(w)/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f') 						
						json_body[0]["time"] = time_db
						json_body[0]["fields"]['watch'] = dict['watches'][w]
						print json_body
						self.influxdb(json_body)
					app_id += 1
				else:
					print "************"
					dict={'thread-%s'%str(int(stats_key[-1])+id):{stats_key[:-1]: {}}}
					list=resp[stats_key].strip('[]').split(',')
					print list			
					for d in list:

#						list[list.index(d)] = d.strip().strip('{}')	
						
						dict['thread-%s'%str(int(stats_key[-1])+id)][stats_key[:-1]][float(d.strip().strip('{}').split(":")[0])] = float(d.strip().strip('{}').split(":")[1])
					print dict
				
					json_body = [{"measurement" : "hZookeeper_stats", "tags":{}, "time":'', "fields" : {}}]
					json_body[0]["tags"]['client'] = 'thread-%s'%str(int(stats_key[-1])+id)

					for t in dict['thread-%s'%str(int(stats_key[-1])+id)].keys():
						
						for k in dict['thread-%s'%str(int(stats_key[-1])+id)][stats_key[:-1]].keys():
#							print int(k)
#							print k
							time_db = datetime.datetime.fromtimestamp(float(k)/1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
#							print time_db
							json_body[0]["time"] = time_db
							json_body[0]["fields"][t] = float(dict['thread-%s'%str(int(stats_key[-1])+id)][stats_key[:-1]][k])
							print json_body			
							self.influxdb(json_body)
			id += self.options.threads_client
		
		print "******************"

	def influxdb(self, json_body):
		client = InfluxDBClient(host='10.10.0.73', port=8086, username='root', password='root', database='hZookeeper')
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
		
	def launch_zk_pub(self):
#		"""
#		Function to launch zookeeper stress app.
#		"""
		print ("Launching the Zookeeper stress app")
		max_threads_per_client = 5
		if self.options.client_count > max_threads_per_client:
#			threads_per_client = max_threads_per_client
			client_count = math.ceil(self.options.client_count / float(max_threads_per_client))
			print "Clients to launch : %s" %int(client_count)
			threads_per_client = int(math.ceil(self.options.client_count / client_count))
			self.options.threads_client = threads_per_client
			print "Threads per client : %s" %threads_per_client
			
			
		else:
			threads_per_client = self.options.client_count
			self.options.threads_client = threads_per_client

		self.create_binary_app(name=self.zk_pub_app_id, app_script='./src/zk_stress.py %s %s %s %s %s %s'
									  % (self.options.znode_creation_count,
									  	 self.options.znode_data,
									  	 self.options.znode_modification_count,
									  	 self.options.stress_reader,
										 self.options.zk_server_ip,
									  	 threads_per_client),
	                               cpus=0.1, mem=128, ports=[0])
		if self.options.client_count > max_threads_per_client:
#			tt= self.options.client_count /float( max_threads_per_client )
#			print tt
#			client_count = math.ceil(self.options.client_count / float(max_threads_per_client))
#			l.info("Number of Zookeeper-Stress Clients to launch = %s" % (client_count))
			self.scale_and_verify_app(self.zk_pub_app_id, client_count)
#			print client_count, self.options.client_count, max_threads_per_client
			print "Done scaling !"
#		time.sleep(20)
class RunTest(object):
	def __init__(self, argv):
        	usage = ('python %prog --znode_creation_count=<Znodes count>'
                	 '--client_count=<Total clients to launch>'
                	 '--znode_data=<Desired data you want to store in a znode>'
                	 '--znode_modification_count=<Number of znodes to modify to trigger watches>'
					 '--stress_reader=<yes or no here>')

        	parser = OptionParser(description='zookeeper scale test master',
        	                      version="0.1", usage=usage)
		parser.add_option("--znode_creation_count", dest='znode_creation_count', default=1000, type='int')
		parser.add_option("--client_count", dest='client_count', default=5, type='int')
		parser.add_option("--znode_data", dest='znode_data', default='Muneeb', type='str')
		parser.add_option("--znode_modification_count", dest='znode_modification_count', default=10, type='int')
		parser.add_option("--stress_reader", dest='stress_reader', default='no', type='str')
		parser.add_option("--zk_server_ip", dest='zk_server_ip', default='10.10.0.73:2181', type='str')
		(options, args) = parser.parse_args()
		if ((len(args) != 0)):
			parser.print_help()
			sys.exit(1)



		print options
#		print time.time()
#		print time.clock()
#		num_msgs = int(argv[1])
#		client_count = int(argv[2])
#		zk_server_ip = argv[3] 

		r = ZK(options)

		r.start_appserver()

		r.run_test()


#	        print ("About to sleep for 15")
#       time.sleep(15)
#		r.delete_all_launched_apps()
		r.stop_appserver()

if __name__ == "__main__":
	RunTest(sys.argv)
