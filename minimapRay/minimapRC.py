import os
import socket
from random import random
from threading import Thread
import time

import ray
import subprocess

from filelock import FileLock
import warnings
warnings.filterwarnings("ignore")

@ray.remote
class minimapR2(object):
    # def __init__(self, reads,tar,options):

    def __init__(self, reads,options,tar):
        self.reads=reads
        self.tar=tar
        self.options=options
        
    def minimap(self):
        # start = time.time()
        result = []
        # print("worker ip: " + str(socket.gethostbyname(socket.gethostname())))
        start = time.time()
        path="/usr/local/tmp/Thread_"+str(os.getpid())+".fastq";
        command =  self.options+self.tar+path
        print(command)
        file = open(path, "w",-1)
        for lines in self.reads:
            line=lines.split()
            for part in line:
                file.write(part)
                file.write("\n")
        file.close()
        # for line in file.readlines():
        #     print("filereads: "+line)
        input_byte = bytes(path.strip(), 'utf8')
        # p=subprocess.Popen("sleep "+str(self.time/10)+"s",shell=True)
        p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,bufsize=1)

        for line in iter(p.stdout.readline, b''):
            if str(line.decode('utf8')).strip() == "minimap_is_finished":
                # print("finishd")
                break
            deline = line.decode("utf8").strip()
            if (deline[0] != '@'):
                result.append(deline)
                # print(deline)
            if not subprocess.Popen.poll(p) is None:
                print("minimap terminated")
                break
        print("minimap " + str(os.getpid()) + " time use: " + str(time.time() - start))
        p.stdout.close()
        # p.terminate()
        # time.sleep(self.time/10)
        p.wait()
        # print("minimap "+str(os.getpid()) + " time use: " + str(time.time() - start))
        return result