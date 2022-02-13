import os
for i in range(3):
    os.system("gnome-terminal -e 'bash -c \"ray start --address='192.168.240.165:6379' --redis-password='5241590000000000'; exec bash\"'")
    