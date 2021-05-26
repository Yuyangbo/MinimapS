import os
from socket import socket
import warnings
warnings.filterwarnings("ignore")
import ray

def fileload(path):
    array = []
    f = open(path, "r",buffering=-1)
    linenum=0
    temparray = ""
    #根据fastq文件格式，每四行为一条序列，将其转为一行存入数组
    for line in f.readlines():
        if(linenum<4):
            temparray=temparray+line.strip()+" "
            linenum+=1
        else:
            array.append(temparray.strip())
            temparray = line.strip() + " "
            linenum=1
    array.append(temparray.strip())
    # print(array)
    return array

