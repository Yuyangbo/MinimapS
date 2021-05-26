import subprocess
import socket

from filelock import FileLock
reads="/home/yyb/Graduation/minimap2R/test/chr21test.fastq"
target="/home/yyb/Graduation/minimap2R/minimap2 "
options="-a /home/yyb/Graduation/minimap2R/test/chr21.fa ray"

input_byte = bytes(reads, 'utf8')
with FileLock("/home/yyb/Graduation/my_data.txt.lock"):
    p= subprocess.Popen(target+options, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,bufsize=1)
    for line in iter(p.stderr.readline,b''):
        # print(str(line.decode('utf8')).strip())
        if str(line.decode('utf8')).strip()=="minimap is ready":
            print("MINI READY")
            break
        if not subprocess.Popen.poll(p) is None:
            print("minimap terminate")
            # if line == "":
            break
    print("Begin socket")
    sk = socket.socket(socket.AF_INET)
    address = ("localhost",7778)
    sk.connect(address)
    print("connected")
    sk.sendall(input_byte)
    sk.close()
    result=[]
    for line in iter(p.stdout.readline, b''):
        if str(line.decode('utf8')).strip() == "minimap_is_finished":
            break
        deline=line.decode("utf8").strip()
        print(deline)
        if(deline[0]!='@'):
            result.append(deline)
        if not subprocess.Popen.poll(p) is None:
            break
    p.stdout.close()
    p.terminate()