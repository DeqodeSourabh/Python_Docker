import ray 
import os 
import time


def start_node():
    os.system("gnome-terminal -e 'bash -c \"ray start --head; exec bash\"'")
    time.sleep(2)


def create_multiple_nodes():
    workers=2
    for i in range(workers):
        os.system("gnome-terminal -e 'bash -c \"ray start --address='192.168.240.165:6379' --redis-password='5241590000000000'; exec bash\"'")
    time.sleep(3)

def stop_node():
    os.system("gnome-terminal -e 'bash -c \"ray stop; exec bash\"'")
