package ccc.example.ClusterTest;


import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Preprocess {
	static DecimalFormat dff = new DecimalFormat("#0.000");

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration cfg = new Configuration();
		FileSystem fs = FileSystem.get(URI.create("hdfs://10.1.14.180:9000/in"), cfg);
		FileStatus files[] = fs.listStatus(new Path("hdfs://10.1.14.180:9000/in"));
		for (FileStatus file : files) {
			System.out.println(file.getPath());
			Path inputpath = file.getPath();
			String[] path = inputpath.toString().split("/");
			String userid = path[path.length - 1];
			String outputpath = "hdfs://10.1.14.180:9000/out/" + userid;
			Configuration conf = new Configuration();
			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "preprocess");
			job.setJarByClass(Preprocess.class);
			job.setMapperClass(PreprocessMapper.class);
			job.setReducerClass(PreprocessReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			DeleOutputPath(outputpath);// 如果输出文件夹存在就删除
			FileInputFormat.addInputPath(job, inputpath);
			FileOutputFormat.setOutputPath(job, new Path(outputpath));
			// System.exit(job.waitForCompletion(true) ? 0 : 1);
			job.waitForCompletion(true);
		}
	

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

	public static class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String data = value.toString();
			String[] part = data.split("\t");
			if (part.length==9) {
				if (!data.split("\t")[1].equals("0")) {
					String itemid = data.split("\t")[2];
					String itemlen = data.split("\t")[6];
					String behavelen = data.split("\t")[7];
					if (!itemlen.equals("")&&!behavelen.equals("")) {
						double percent = Double.parseDouble(behavelen) / Double.parseDouble(itemlen);
						context.write(new Text(itemid), new Text(dff.format(percent)));
					}
					
				}
			}
			
		}
	}

	public static class PreprocessReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double percent = 0;
			for (Text val : values) {
				if (percent < Double.parseDouble(val.toString())) {
					percent = Double.parseDouble(val.toString());
				}
			}
			context.write(key, new Text(dff.format(percent)));
		}

	}

}