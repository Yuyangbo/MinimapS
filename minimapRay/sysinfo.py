import socket
import time
from collections import Counter

import ray

def getclusterinfo():
    print('''This cluster consists of
        {} nodes in total
        {} CPU resources in total
    '''.format(len(ray.nodes()), int(ray.cluster_resources()['CPU'])))

@ray.remote
def gethostip():
    time.sleep(0.001)
    object_ids = socket.gethostbyname(socket.gethostname())
    return object_ids

def ipinfo():
    ids =[gethostip.remote() for _ in range(10)]
    ip_addresses = ray.get(ids)
    print('Tasks executed')
    for ip_address, num_tasks in Counter(ip_addresses).items():
        print('    {} tasks on {}'.format(num_tasks, ip_address))


