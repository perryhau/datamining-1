package cluster;

import java.io.IOException;
import java.util.List;

import lib.CenterUtils;
import lib.Contants;
import lib.Point;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CentroidRunJob extends ToolJob {

	@Override
	public int run(String[] args) throws Exception {
		String input = args[0];
		String centroidFile = args[1];
		int k = Integer.parseInt(args[2]);
		int maxTimes = Integer.parseInt(args[3]);
		
		String originalCentroid = centroidFile+"/"+Contants.CENTROID_FOLDER_NAME+0;
		String newCentroid = centroidFile+"/"+Contants.CENTROID_FOLDER_NAME+1;
		
		//生成初始随机中心点
		CenterUtils.createRandomCenter(k, input, originalCentroid);

		//第一次cluster，计算新的中心点
		computeCentroidRun(input,originalCentroid,newCentroid);
		List<Point> oldCenterList = CenterUtils.readPoint(originalCentroid);
		List<Point> newCenterList = CenterUtils.readPoint(newCentroid);
		
		//循环计算新的中心点
		double diff = CenterUtils.compareCentorid(oldCenterList, newCenterList);
		int time = 2;
		if (diff >10 || time>maxTimes) {
			String oldCentroid = newCentroid;
			newCentroid = centroidFile+"/"+Contants.CENTROID_FOLDER_NAME+time;
			computeCentroidRun(input,oldCentroid,newCentroid);
			oldCenterList = CenterUtils.readPoint(oldCentroid);
			newCenterList = CenterUtils.readPoint(newCentroid);
			diff = CenterUtils.compareCentorid(oldCenterList, newCenterList);
			time++;
		}
		
		return 1;
	}
	
	public void computeCentroidRun(String inputPointFile, String originalCentroid, String newCentroid) {
		Configuration conf = getConf();
		try {
			Job job = new Job(conf,"computeCentroid");
			job.setMapperClass(CentroidMapper.class);
			job.setMapOutputKeyClass(Point.class);
			job.setMapOutputValueClass(Point.class);
			job.setReducerClass(CentroidReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Point.class);
			
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
