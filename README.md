# MinimapS
MinimapS is a tool which accelerates minimap2 on Spark and Ray cluster respectively.

#### Table of Contents
1. [Description](#Description)
2. [Setting up](#Settingup)
3. [Test](#Demo)
4. [FAQs](#FAQs)

<a name="Description"></a>
I. Description
----
MinimapS is a tool which accelerates minimap2 on Spark and Ray cluster respectively.

minimap2 is a sequence alignment tool in the field of Bioinformatics.  With the  development of the Gene Sequencing technology, reads of DNA or other kinds of sequence become longer and more numerous and minimap2 can take more time for the aligning job.  To help speed up the tool,  I improve the tool by applying it on Spark and Ray which use parallel computing technology on clusters: minimapS.

The advantages of minimapS compared with minimap2:

* Less time used in file setting on hdfs file system with Spark.
* Higher performance when using  massive reads.

<a name="Settingup"></a>
II. Setting up
----
MinimapS can also be installed in a local computer or a cluster.
MinimapS consists of two parts, minimapS and minimapR. minimapS is designed for use on Spark and minimapR is designed for use on Ray.

### Step1:
Download MinimapS source code, including modified minimap2 with source code called minimap2R.

### Step2:
To run  MinimapS you have to set up several variants on your computer or cluster. 

If using Spark,  Java, Scala,  hadoop with yarn cluster manager and hdfs file system and configured Spark source are needed. You need to make your own runing environment according to your RAM, CPU cores and number of computers in cluster. You may using virtual machines or docker to simulate clusters.
Some example settings for yarn and Spark are listed below.
Following is the settings of yarn-site.xml, make sure you allocate enough RAM for nodemanager.
```
<property>
		<name>yarn.resourcemanager.hostname</name>
		<value>master</value>
</property>
<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
</property>
<property>
		<name>yarn.log-aggregation-enable</name>
		<value>true</value>
</property>
<property>
		<name>yarn.log-aggregation.retain-seconds</name>
		<value>604800</value>
</property>
<property>
    	<name>yarn.nodemanager.resource.memory-mb</name>
    	<value>81920</value>
</property>
<property>
    	<name>yarn.scheduler.minimum-allocation-mb</name>
    	<value>2048</value>
</property>
<property>
    	<name>yarn.scheduler.maximum-allocation-mb</name>
    	<value>81920</value>
</property>
```
And for spark-env.sh, it is crucial to set proper RAM use and the location of your environment variables
```
export JAVA_HOME=/usr/local/envir/jdk1.8.0_291
export SCALA_HOME=/usr/local/envir/scala-2.13.5
export HADOOP_HOME=/usr/local/envir/hadoop-3.2.2
export HADOOP_CONF_DIR=/usr/local/envir/hadoop-3.2.2/etc/hadoop

SPARK_MASTER_IP=master
SPARK_WORKER_MEMORY=81920m
```

If using Ray, only Python3 with some dependent packages are needed.
You can use following commands to install Ray for later use.
```
pip install -U ray
```

<a name="Demo"></a>

III. Test
----
### Step1 Prepare dataset:
Using pbsim tool and execute following script to generate query sequence(reads) fastq format file with different amounts of data. The variable  d means the depth of  sequence and the higher d is set, the more sequences you can get.
```
pbsim --prefix xxx --data-type CLR --depth d --model_qc /data/model_qc_clr xxx.fa
```
Launch hadoop and upload the reads file(fastq format) to hdfs using command:
```
hdfs dfs -put sourcefile /destination(on hdfs)
```
Of course, you will not need this step if only test on Ray.

### Step2 Working on Spark:
Make sure you have launched yarn cluster you need or Spark standlone cluster.
Using spark submit command to execute minimapS packed as jar.
For example, following script uses MinimapS with 64 exeutors, each has 4 cores to compute parallel.  Driver memory need to be set bigger than the fastq file while the executor memory also need to be set bigger than the capacity of split file.  The number of split file is set as the variable in the program as n. You also need to use '-I' to set the location of fastq file and '-O' of output file. 
```
spark-submit --class MinimapS --master yarn\
--executor-cores 4\
--num-executors 64\
--driver-memory 2g\
--executor-memory 2g\
/usr/local/resource/MinimapS.jar\
-master master\
-n 64\
-mini2 /usr/local/resource/minimap2R/minimap2\
-a /usr/local/resource/minimap2R/test/chr21.fa\
-I hdfs://master:9000/minimapS/QuerySeq/chr21depth10.fastq\
-O hdfs://master:9000/minimapS/out
```
If you want to use local computer without cluster, you can use following scripts:(You mayt also need to start hadoop because minimapS will always use hdfs to save file)
```
spark-submit --class MinimapSlocal 
--master master\
/usr/local/resource/MinimapS.jar\
--driver-memory 10g\
--executor-memory 10g\
-master local[8]\
-n 8 -I hdfs://master:9000/minimapS/QuerySeq/chr21depth10.fastq\
-O hdfs://master:9000/minimapS/out
```

You may see the whole workflow outputs or errors in shell and also the time minimapS used to process minimap2.
Finally, you can find the results in hdfs and they are splitted to different parts. You can download them and use cat instruction to combine.

### Step3 Working on Ray:
First you need to launch ray cluster or just run the program on single machine.
To start a ray cluster, use following scripts as examples:

```
# To start a head node.
ray start --head  --num-cpus=1 --num-gpus=0

# To start a non-head node, every node needs to do this.
ray start --address=192.168.80.132:6379 --num-cpus=1 --num-gpus=0
```

Then you can execute minimapRay using scripts:
```
#For local environment, specify number of cores you need to use, the input-file path, the command, the reference file and the output-file path:
python3 Raylocal.py 8 /test/chr21depth3.fastq 'minimap2 -a' '/test/chr21.fa ' /result

#For cluster environment, you need to modify the source code RayCluster.py with a change of the cluster address and then specify number of cores you need to use, the input-file path, the command, the reference file and the output-file path:
python3 RayCluster.py 8 /test/chr21depth3.fastq 'minimap2 -a' '/test/chr21.fa ' /result
```

<a name="FAQs"></a>
IV. FAQs
----
### Error: 
If the error *java heap error* or *no resource accept task*  is reported when using Spark, please check whether you have allocated enough RAM :

#Step1: Change the settings in yarn-site.xml in all cluster members and restart hadoop and Spark:

#Step2: If the problem continus occuring, change the settings in spark-env.sh and give more RAM space that Spark can use.
