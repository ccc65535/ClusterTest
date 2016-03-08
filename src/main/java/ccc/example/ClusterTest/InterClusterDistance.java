package ccc.example.ClusterTest;




import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.Cluster;
//import org.apache.hadoop.mapreduce.Cluster;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;

public class InterClusterDistance {

	public static void main(String[] args) throws Exception{
		String inputFile="";
		Configuration conf=new Configuration();
		Path path=new Path(inputFile);
		System.out.println("Input Puth="+path);
		FileSystem fs=FileSystem.get(path.toUri(),conf);
		List<Cluster> clusters=new ArrayList<Cluster>();
		SequenceFile.Reader  reader=new SequenceFile.Reader(fs, path,conf);
		Writable key=(Writable)reader.getKeyClass().newInstance();
		Writable value=(Writable)reader.getValueClass().newInstance();
		while(reader.next(key, value)){
			Cluster cluster=(Cluster)value;
			clusters.add(cluster);
			value=(Writable)reader.getValueClass().newInstance();
		}
		DistanceMeasure measure=new CosineDistanceMeasure();
		double max=0;
		double min=Double.MAX_VALUE;
		double sum=0;
		int count=0;
		for(int i=0;i<clusters.size();i++){
			for(int j=i+1;j<clusters.size();i++){
				double d=measure.distance(clusters.get(i).getCenter(),clusters.get(j).getCenter());
				min=Math.min(d, min);
				max=Math.max(d, max);
				sum+=d;
				count++;
			}
		}
		System.out.println("Maximum InterCluster DistanceL"+max);
		System.out.println("Minimum InterCluster DistanceL"+min);
		System.out.println("Average InterCluster DistanceL(Scaled):"+(sum/count-min)/(max-min));
	}
}
