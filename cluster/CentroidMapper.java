package cluster;

import java.io.IOException;
import java.util.List;

import lib.Contants;
import lib.CenterUtils;
import lib.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import diatance.EuclideanDistance;

public class CentroidMapper extends Mapper<Text,Point,Point,Point>{

	private List<Point> centerList;
	
	@Override
	public void map(Text key, Point point, Context context) {
		double shortest = Double.MAX_VALUE;
		Point shortestCenter = null;
		for (Point center : centerList) {
			double distance = new EuclideanDistance().computeDistance(point, center);
			if (distance < shortest) {
				shortest = distance;
				shortestCenter = center;
			}
		}
		
		if (shortestCenter == null	) 
			return;
		
		try {
			context.write(shortestCenter, point);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String centerFile = conf.get(Contants.CENTROID_FILE);
		
		try {
			centerList = CenterUtils.readPoint(centerFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
