package ccc.example.ClusterTest;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


//hadoop数据处理结果转成列表
public class fina {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "zj");
		job.setJarByClass(fina.class);
		job.setMapperClass(clusterMapper.class);
		job.setReducerClass(clusterReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		DeleOutputPath("hdfs://10.1.14.180:9000/cluster/output");
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.1.11:9000/cluster/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.11:9000/cluster/output"));
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.waitForCompletion(true);

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
		} catch (Exception e) {			System.out.println(e);
		}
	}

	public static class clusterMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line.startsWith("Key:")) {
				String Key = line.split(": ")[1];
				String vector = line.split(": ")[4];
				String[] newvector = new String[14];
				for (int i = 0; i < 14; i++) {
					newvector[i]="0.000";
				}
				if (!vector.equals("[]")) {
//					vector = vector.replaceAll("[", "");
//					vector = vector.replaceAll("]", "");
					vector = vector.substring(1, vector.length()-1);
					String[] vec = vector.split(", ");

					for (int i = 0; i < vec.length; i++) {
						int index = Integer.parseInt(vec[i].split(":")[0]);
						newvector[index]=	vec[i].split(":")[1];
					}
				}
				vector=newvector[0];
				for (int i = 1; i < 14; i++) {
					vector = vector+" "+newvector[i];
				}
				
				context.write(new Text(Key), new Text(vector));
			}
			
		}
	}

	public static class clusterReducer extends
			Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> outputs ;
		protected void setup(Context context){
			outputs = new MultipleOutputs<Text, Text>(context);
		}
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				outputs.write(val, new Text(""), key.toString());
			}

		}
		public void cleanup(Context cxt) throws IOException, InterruptedException{
			outputs.close();
		}

	}

}