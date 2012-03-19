package test;

import java.io.IOException;
import java.util.List;

import lib.CenterUtils;
import lib.Point;
import lib.ToolJob;

import org.apache.hadoop.conf.Configuration;

public class File extends ToolJob{

	public static void main (String[] args) {
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		CenterUtils centerUtils = new CenterUtils(conf);
		
		String url = args[0];
//		String url = "data/click3.seq";
		try {
			List<Point> pointList = centerUtils.readPoint(url);
			for (int i=0; i<10; i++) {
				System.out.println(pointList.get(i));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 1;
	}
}
