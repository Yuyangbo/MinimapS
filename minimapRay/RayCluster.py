import os
import time

import ray as ray

import sysinfo
import numpy as np
import ray
import fileoperation
import minimapR2
import sys
ray.init(address='172.17.0.2:6379')
nodenum=int(sys.argv[1])
#ray.init(address="10.88.0.10:6379")
#ray.init(address='172.17.0.7:6379')
# nodenum=int(ray.cluster_resources()['CPU'])
# sysinfo.getclusterinfo()
# sysinfo.ipinfo()

# start=time.time()
readslist=fileoperation.fileload(sys.argv[2])
# print("Time reads: ",time.time()-start)

# lines=[]
# lines.append(reads.get())
#options="/usr/local/resource/minimap2R/minimap2 -a "
#target="/usr/local/resource/minimap2R/test/chr21.fa "
options=sys.argv[3]
target=sys.argv[4]
#dest=sys.argv[5]
readslength=len(readslist)


tempreads=[]
fileactors=[]
# start=time.time()

count=0
for i in range(nodenum):
    tempreads = []
    for reads in readslist[count:int(readslength/nodenum+count)]:
        tempreads.append(reads)
        count+=1
    if(i==nodenum-1):
        for reads in readslist[count:]:
            tempreads.append(reads)
    # print("part reads: ")
    # print(tempreads)
    tempactor=minimapR2.minimapR2.remote(tempreads,options,target,i)
    fileactors.append(tempactor)

start=time.time()
core=0

for c in fileactors:
    c.minimap.remote()

result=ray.get([c.fin.remote() for c in fileactors])

print("Ray with " + str(nodenum) + " cores,time use: " + str (time.time()-start))


#     fileactors.append(fileoperation.savefile.remote(reads))
# fileactor=ray.get(fileoperation.savefile.remote(reads))
# fileactor.save_in_nodes.remote()
# moniactor=minimap.minimapR.remote(target,options)
# moniactor.minimap.remote()