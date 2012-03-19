package cluster;

import java.io.IOException;

import lib.Contants;
import lib.Point;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class RunCentroid extends ToolJob {


	@Override
	public int run(String[] args) throws Exception {
		String inputPointFile = args[0];
		String originalCentroid = args[1];
		String newCentroid = args[2];
		
		Configuration conf = getConf();
		conf.set(Contants.CENTROID_FILE, originalCentroid);
		try {
			Job job = new Job(conf,"computeCentroid");
			job.setJarByClass(RunCentroid.class);
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
		
		return 1;
	}
}
