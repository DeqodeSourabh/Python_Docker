import ray 
import os 
import time

port = 6366
workers_ips = []  #This we can use when we have workers ip Or deploy On cloud
head_ip= "192.168.240.165"
max_workers = 5
min_workers = 0

def start_node():
    stop_node()
    os.system("gnome-terminal -e 'bash -c \"ray start --head --port %d ; exec bash\"'"%(port))
    time.sleep(2)

def create_multiple_nodes():
    for i in range(max_workers):
        os.system("gnome-terminal -e 'bash -c \"ray start --address='%s:%d' --redis-password='5241590000000000'; exec bash\"'" %(head_ip,port))
    time.sleep(3)

def stop_node():
    os.system("gnome-terminal -e 'bash -c \"ray stop; exec bash\"'")

