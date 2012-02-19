package my;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import diatance.Distance;

public class KMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
	
	private Distance distanceMeasure;
	@Override
	protected void map(LongWritable key, Text value, Context context) {
		
	}

	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		try {
			distanceMeasure = classLoader.loadClass(conf.get("distanceClass")).asSubclass(Distance.class).newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
