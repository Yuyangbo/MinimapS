import PreProcess.FileOperator;
import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;

import PreProcess.Seqdata;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

public class MinimapS {
	static String ip;
	static String inputName;
	static String outputName;
	static int nodenum;
	static String command;
	static boolean isReady = false;
	public static void main(String[] args) throws IOException {
		JavaRDD<String> filereads = null;
		JavaRDD<String> wholeFile = null;
		String commands = "MinimapS: ";
		for(int i = 0 ; i < args.length ; i++) {
			commands+=" "+args[i];
		}
		System.out.println(commands);
		String appName=new String();
		for (int i = 0; i < args.length; i++) {
			System.out.println(args[i]);
			switch (args[i].toUpperCase()) {
				case "-MASTER":
					ip=args[++i];
					System.out.println(ip);
					break;
				case "-MINIMAPS":
					break;
				case "-H":
					helpinfo();
					break;
				case "-I":
					inputName = args[++i];
					System.out.println(inputName);
					break;
				case "-O":
					outputName = args[++i];
					System.out.println(outputName);
					break;
				case "-N":
					nodenum= Integer.parseInt(args[++i]);
					System.out.println(nodenum);
					break;
				case "-mini2":
					command= args[++i]+" ";
					break;
			}
		}
		SparkConf conf = new SparkConf().setAppName(appName);
//		conf.set("spark.hadoop.validateOutputSpecs", "false");
//		String master="spark://"+ip+":7077";
//		String master=ip;
//		System.out.println(master);
//		conf.setMaster(master).setJars(new String[]{"J:\\Graduation\\MinimapS\\target\\MinimapS.jar"});
//		conf.setMaster(master).setJars(new String[]{"/usr/local/resource/MinimapS.jar"});
		JavaSparkContext spark = new JavaSparkContext(conf);
//		inputName="hdfs://192.168.80.138:9000/minimap2/chr21_1.fastq.gz";
//		long startTime = System.currentTimeMillis();
		if (inputName.substring(inputName.lastIndexOf(".") + 1).equals("gz")){
			System.out.println("This is a fastq gz file");
			wholeFile = spark.textFile(FileOperator.gzfile(inputName,ip));
			System.out.println("file partitions num: "+wholeFile.getNumPartitions());
			filereads = Readfastq(wholeFile);
		}
		else if(inputName.substring(inputName.lastIndexOf(".") + 1).equals("fa")||inputName.substring(inputName.lastIndexOf(".") + 1).equals("fasta")){
			System.out.println("This is a fasta file");
			wholeFile = spark.textFile(inputName,nodenum);
			System.out.println("file partitions num: "+wholeFile.getNumPartitions());
			filereads = Readfasta(wholeFile);
		}
		else if (inputName.substring(inputName.lastIndexOf(".") + 1).equals("fastq")) {
			System.out.println("This is a fastq file");
			wholeFile = spark.textFile(inputName,nodenum);
			System.out.println("file partitions num: "+wholeFile.getNumPartitions());
			filereads = Readfastq(wholeFile);
		}
//		wholeFile.persist(StorageLevel.MEMORY_AND_DISK_SER());
//		JavaPairRDD<Long,Tuple2<Integer,String>> pair = null;
		System.out.println("file part: "+filereads.getNumPartitions());
		JavaRDD<String> result =null;
		filereads.persist(StorageLevel.MEMORY_ONLY());
		long startTime = System.currentTimeMillis();
		result = miniMap(filereads);
//		long endTime = System.currentTimeMillis();
//		System.out.println("minimap process time: " + (endTime - startTime) + " ms");
//		JavaPairRDD<String,String> pairresult = miniMap(filereads).mapToPair(x->{
//			String[] data=x.split(" ");
//			Tuple2<String,String> pair=new Tuple2<String,String>(data[0],x);
//			System.out.println("data[0] in pair"+x);
//			return pair;
//		});
//		System.out.println("file: "+filereads.collect());
////		results.persist(StorageLevel.MEMORY_ONLY());
		FileOperator.DeleteHadoopFile(outputName,ip);
////		System.out.println("debug result: "+results.toDebugString());
////		outputName="hdfs://192.168.80.138:9000/minimap2/out";
//
//		JavaRDD<String> sortedresult = pairresult.repartitionAndSortWithinPartitions(new HashPartitioner(1)).map(x->{
//			System.out.println("x "+x+"x1 2: "+x._1+x._2);
//			return x._2;
//		});
//		System.out.println("sortresult: "+sortedresult.collect());
//		System.out.println("result part: "+sortedresult.getNumPartitions());
//		sortedresult.repartition(1).saveAsTextFile(outputName);
		result.saveAsTextFile(outputName);
//		System.out.println("result: "+result.collect());
//		result.collect();
		long endTime = System.currentTimeMillis();
		System.out.println("minimap process time: " + (endTime - startTime) + " ms");
		spark.close();

	}

	private static JavaRDD<String> Readfastq(JavaRDD<String> fastqReads){
		return fastqReads.zipWithIndex().mapToPair(x -> {
//			System.out.println("count mapTopair: "+count0);
//			count0++;
//			System.out.println("x_1,x_2:"+ x._1+"  "+x._2);
			Tuple2<Long, Tuple2<Integer, String>> tp = new Tuple2<Long, Tuple2<Integer, String>>(x._2/4, new Tuple2<Integer, String>((int)(x._2%4), x._1));
//			System.out.println("tuple tp:"+ tp);
			return tp;
		}).groupByKey().map(x -> {
			String rec=new String();
			String[] lines = new String[4];
			for(Tuple2<Integer, String> tp : x._2) {
//				System.out.println("tp._2:"+ tp._2);
				lines[tp._1]=tp._2;
			}
//			System.out.println("lines result:");
//			for(int m=0;m< lines.length;m++){
//				System.out.println(lines[m]);
//			}
			if(lines[0]==null || lines[1]==null || lines[3]==null) {
				System.out.println("file format is wrong");
				return null;
			}
			if(lines[0].charAt(0)=='@') {
				int end = lines[0].indexOf(" ");
				if(end<0) end = lines[0].length();
				rec = lines[0].substring(0,end) + " " + lines[1] + " " + lines[3];
			}
//			System.out.println("rec："+rec);
			return rec;
		});
	}

	private static JavaRDD<String> Readfasta(JavaRDD<String> fastaReads){
		return fastaReads.zipWithIndex().mapToPair(x -> {
//			System.out.println("count mapTopair: "+count0);
//			count0++;
			System.out.println("x_1,x_2:"+ x._1+"  "+x._2);
			Tuple2<Long, Tuple2<Integer, String>> tp = new Tuple2<Long, Tuple2<Integer, String>>(x._2/2, new Tuple2<Integer, String>((int)(x._2%2), x._1));
//			System.out.println("tuple tp:"+ tp);
			return tp;
		}).groupByKey().map(x -> {
			String rec="";
			String[] lines = new String[2];
			for(Tuple2<Integer, String> tp : x._2) {
//				System.out.println("tp._2:"+ tp._2);
				lines[tp._1]=tp._2;
			}
			System.out.println("lines result:");
			for(int m=0;m< lines.length;m++){
				System.out.println(lines[m]);
			}
			if(lines[0]==null || lines[1]==null) {
				System.out.println("file format is wrong");
				return null;
			}
			if(lines[0].charAt(0)=='>') {
				int end = lines[0].indexOf(" ");
				if(end<0) end = lines[0].length();
				rec = lines[0].substring(0,end) + " " + lines[1];
			}
//			System.out.println("rec："+rec);
			return rec;
		});
	}

	private static JavaRDD<String> miniMap(JavaRDD<String> records){
		return records.mapPartitions(x -> {
			ArrayList<String> result = new ArrayList<String>();
			long ThreadID = Thread.currentThread().getId();
			String fileName = "TempDataId_"+ThreadID+".fastq";
			File tempFile = new File(fileName);
			System.out.println("file path"+tempFile.getAbsolutePath());
			FileWriter tempFileWriter = new FileWriter(tempFile);
			BufferedWriter bw = new BufferedWriter(tempFileWriter);
			ArrayList<Seqdata> linedatas = new ArrayList<>();
			final int max=1000;
			int count = 0;
			int writeToFile = 0;
			while(x.hasNext()){
				String eachline = x.next();
				String splitline[] = eachline.split(" ");
				if(splitline.length != 3) {
					System.out.println("ERROR READS: "+ splitline);
					continue;
				}
				Seqdata linedata=new Seqdata(splitline[0],splitline[1],splitline[2]);
				linedatas.add(linedata);
//				System.out.println("seq in minimap"+linedatas.get(count));
				count++;
				if(count==max) {
					for(int i = 0 ; i< count; i++) {
						bw.write(linedatas.get(i).getSeqname()+"\n");
						bw.write(linedatas.get(i).getSequance()+"\n");
						bw.write("+\n");
						bw.write(linedatas.get(i).getQuality()+"\n");
						writeToFile++;
					}
					count=0;
				}
			}
			for(int i = 0 ; i< count; i++) {
				bw.write(linedatas.get(i).getSeqname()+"\n");
//				System.out.println("distribute name: "+linedatas.get(i).getSeqname());
				bw.write(linedatas.get(i).getSequance()+"\n");
				bw.write("+\n");
				bw.write(linedatas.get(i).getQuality()+"\n");
				writeToFile++;
			}
			bw.flush();
			bw.close();
			System.out.println(ThreadID+"write "+writeToFile+" lines to files");
			int cnt = 0;
//			final Process p = Runtime.getRuntime().exec("mpirun -np 1 /home/hadoop/Graduation/minimap2S/minimap2 -a /home/hadoop/Graduation/minimap2S/test/chr21.fa minimos");
			String absPath = tempFile.getAbsolutePath();
			System.out.println("temppath: "+absPath);
			long ministart = System.currentTimeMillis();
			final Process p = Runtime.getRuntime().exec("/usr/local/resource/minimap2R/minimap2 -a -t 1 "+"/usr/local/resource/minimap2R/test/chr1.fa "+absPath);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					p.destroy();
				}
			});
			try {
//
				BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				final BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
				try {
					String stderr = null;
					boolean ready=false;
//						while (!ready) {
//							Thread.sleep(1000);
//						}
					String line = stdInput.readLine();
					while (line != null && !line.equalsIgnoreCase("minimap_is_finished")) {
						if (line.charAt(0) != '@') {
							result.add(line);
						}
//							System.out.println("replay from minimap: "+line);
						line = stdInput.readLine();
					}

				} catch (Exception e) {
					e.printStackTrace();
					File delFile = new File(absPath);
					if (delFile.exists()) {
						delFile.delete();
					}
				}
			}catch (Exception e) {
				System.out.println("reading reads ERROR");
				e.printStackTrace(System.err);
			}
			if(tempFile.exists())
				tempFile.delete();
			long miniend = System.currentTimeMillis();
			System.out.println("minimap process time: " + (miniend - ministart) + " ms");
			System.out.println("return to MinimapS main");
			return result.iterator();
		});
	}

	private static void helpinfo(){
		System.out.println("usage sample:\tspark-submit --class MinimapS --master [masterip] --executor-memory 10G --dirver-memory 2G MinimapS.jar minimaps -I [inputfile] -O out.sam");
		System.out.println("");
		System.out.println("MASTER:\t\tIdentify Spark Master local, yarn or ip of spark standalone master");
		System.out.println("");
		System.out.println("inputfile:\tInput reads ");
	}

}
