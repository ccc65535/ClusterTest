package ccc.example.ClusterTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NewProcess {

	static DecimalFormat dff = new DecimalFormat("#0.000");
	static String userid;
	static float sum;

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration cfg = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.1.11:9000/in"), cfg);
		FileStatus files[] = fs.listStatus(new Path("hdfs://192.168.1.11:9000/in"));
		for (FileStatus file : files) {
			sum=0;
			System.out.println(file.getPath());
			Path inputpath = file.getPath();
			String[] path = inputpath.toString().split("/");
			userid = path[path.length - 1];
			String outputpath = "hdfs://192.168.1.11:9000/tempout/" + userid;
			Configuration conf = new Configuration();
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "preprocess");
			job.setJarByClass(NewProcess.class);
			job.setMapperClass(ProcessMapper.class);
			job.setReducerClass(ProcessReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			DeleOutputPath(outputpath);// 如果输出文件夹存在就删除
			FileInputFormat.addInputPath(job, inputpath);
			FileOutputFormat.setOutputPath(job, new Path(outputpath));
			job.waitForCompletion(true);
		}
		String in = "hdfs://192.168.1.11:9000/tempout";
		String out = "hdfs://192.168.1.11:9000/newout";
//		Configuration conf = new Configuration();
//		@SuppressWarnings("deprecation")
//		Job job = new Job(conf, "preprocess");
//		job.setJarByClass(NewProcess.class);
//		job.setMapperClass(SecondProcessMapper.class);
//		job.setReducerClass(SecondProcessReducer.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		DeleOutputPath(out);// 如果输出文件夹存在就删除
//		
//		FileSystem fs1 = FileSystem.get(URI.create(in), cfg);
//		FileStatus files1[] = fs1.listStatus(new Path(in));
//		for (FileStatus file : files1) {
//			Path inputpath = file.getPath();
//			FileInputFormat.addInputPath(job, inputpath);
//		}
//		FileOutputFormat.setOutputPath(job, new Path(out));
//		System.exit(job.waitForCompletion(true) ? 0 : 1);

		processinlocal(in, out);
		

	}

	public static void DeleOutputPath(String uri) {
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			if (fs.exists(path)) {
				fs.delete(path, true);
				System.out.println("delete file!");
			}
		} catch (Exception e) {
			System.out.println("删除文件异常！");
		}
	}

	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String data = value.toString();
			String[] part = data.split("\t");//将字符串分割为子字符串，然后将结果作为数组返回
			if (part.length == 9) {
				if (data.split("\t")[1].equals("2")) {
					String behavelen = data.split("\t")[7];
					if (Integer.parseInt(behavelen) > 60) {
						String type = data.split("\t")[4];
						if (!type.equals("")) {
							context.write(new Text(type), new FloatWritable(1));
							sum++;
						}
						
					}
				}

			}
		}

	}

	public static class ProcessReducer extends Reducer<Text, FloatWritable, Text, Text> {

		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		
				float count=0;
				
				for (FloatWritable val : values) {
					count++;
				}
				context.write(new Text(userid), new Text(key.toString()+"\t"+dff.format(count/sum)));
			
			
		}

	}
	public static class SecondProcessMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String data = value.toString();
			String[] part = data.split("\t");
			context.write(new Text(part[0]), new Text(part[1]));
		}

	}

	public static class SecondProcessReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			float[] percent = new float[14];
			for (int i = 0; i < 14; i++) {
				percent[i]=0;
			}
			for (Text val : values) {
				String data = val.toString();
				String type = data.split("\t")[0];
				percent[Integer.parseInt(type)-1]= Float.parseFloat(data.split(",")[1]);
			}
			String value =String.valueOf(percent[0]);
			for (int i = 1; i < 14; i++) {
				value=value+","+percent[i];
			}
			context.write(key, new Text(value));
		}

	}
	
	public static void processinlocal(String inpath,String outpath) throws IOException{
		Configuration cfg = new Configuration();
		
		FileSystem outfs = FileSystem.get(URI.create(outpath), cfg);
		Path outputpath = new Path(outpath);
		if (outfs.exists(outputpath)) {
			outfs.delete(outputpath,true);
			System.out.println("delete file!");
		}
		FSDataOutputStream out = outfs.create(outputpath);
		
		
		FileSystem infs = FileSystem.get(URI.create(inpath), cfg);
		FileStatus files[] = infs.listStatus(new Path(inpath));
		for (FileStatus file : files) {
			Path inputpath = new Path(file.getPath().toString()+"/part-r-00000");
			
			String[] path = file.getPath().toString().split("/");
			String userid = path[path.length - 1];
			
			String[] percent = new String[14];
			for (int i = 0; i < 14; i++) {
				percent[i]= "0.000";
			}
			
			if (infs.exists(inputpath)) {
				FSDataInputStream input = infs.open(inputpath);
				InputStreamReader inreader = new InputStreamReader(input);
				BufferedReader buffer = new BufferedReader(inreader);	
				String line = "";
				while ((line=buffer.readLine())!=null) {
					String type = line.split("\t")[1];
					String per = line.split("\t")[2];
					percent[Integer.parseInt(type)-1]= per;
				}
//				String outdata =userid+"\t"+String.valueOf(percent[0]);
				String outdata =String.valueOf(percent[0]);
				for (int i = 1; i < 14; i++) {
					outdata=outdata+" "+percent[i];
				}
				out.write(outdata.getBytes());
				out.write("\n".getBytes());
				
				buffer.close();
				inreader.close();
				input.close();
			}
			else {
				
//				String outdata =userid+"\t"+String.valueOf(percent[0]);
				String outdata =String.valueOf(percent[0]);
				for (int i = 1; i < 14; i++) {
					outdata=outdata+" "+percent[i];
				}
				out.write(outdata.getBytes());
				out.write("\n".getBytes());
			}
			
		}
		out.close();
	}

}