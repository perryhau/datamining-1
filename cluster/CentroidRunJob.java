package cluster;

import java.io.IOException;
import java.util.List;

import lib.CenterUtils;
import lib.Contants;
import lib.Point;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class CentroidRunJob extends ToolJob {

	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String centroidFile = args[1];
		int k = Integer.parseInt(args[2]);
		int maxTimes = Integer.parseInt(args[3]);
		
		String originalCentroid = centroidFile+"/"+Contants.CENTROID_FOLDER_NAME+0;
		String newCentroid = centroidFile+"/"+Contants.CENTROID_FOLDER_NAME+1;
		
		//
		CenterUtils centerUtils = new CenterUtils(getConf());
		centerUtils.createRandomCenter(k, input, originalCentroid);
		System.out.println("createRandomCenter over");
		//initial cluster
		computeCentroidRun(input,originalCentroid,newCentroid);
		List<Point> oldCenterList = centerUtils.readPoint(originalCentroid);
		List<Point> newCenterList = centerUtils.readPoint(newCentroid);
		
		System.out.println("initial cluster over");
		
		//
		double diff = centerUtils.compareCentorid(oldCenterList, newCenterList);
		int time = 2;
		if (diff >10 || time>maxTimes) {
			System.out.println("round -----"+time);
			String oldCentroid = newCentroid;
			newCentroid = centroidFile+"/"+Contants.CENTROID_FOLDER_NAME+time;
			computeCentroidRun(input,oldCentroid,newCentroid);
			oldCenterList = centerUtils.readPoint(oldCentroid);
			newCenterList = centerUtils.readPoint(newCentroid);
			diff = centerUtils.compareCentorid(oldCenterList, newCenterList);
			time++;
		}
		
		return 1;
	}
	
	public void computeCentroidRun(String inputPointFile, String originalCentroid, String newCentroid) {
		Configuration conf = getConf();
		conf.set(Contants.CENTROID_FILE, originalCentroid);
		try {
			Job job = new Job(conf,"computeCentroid");
			job.setJarByClass(CentroidRunJob.class);
			job.setMapperClass(CentroidMapper.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Point.class);
			job.setReducerClass(CentroidReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Point.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(inputPointFile));
			FileOutputFormat.setOutputPath(job, new Path(newCentroid));
			
			job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
