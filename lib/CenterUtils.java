package lib;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import diatance.EuclideanDistance;

public class CenterUtils {

	public static void createRandomCenter(int k, String pointFile, String outputFile) {
		List<Point> centerList ;
		try {
			centerList = randomInitialCenter(k,pointFile);
			writeCenter(centerList,outputFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static double compareCentorid(List<Point> centerList1, List<Point> centerList2) {
		if (centerList1.size() != centerList1.size()) {
			return -1.;
		}
		int size = centerList1.size();
		double var = 0;
		EuclideanDistance distance = new EuclideanDistance();
		for (int i=0; i<size; i++) {
			var += distance.computeDistance(centerList1.get(i), centerList2.get(i));
		}
		
		return var;
	}
	
	public static void writeCenter(List<Point> centerList, String outputFile) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(outputFile),conf);
		
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(outputFile), Text.class, Point.class);
		for (Point center : centerList) {
			writer.append(new Text("center"), center);
		}
		writer.close();
		fs.close();
		
	}
	
	public static List<Point> randomInitialCenter(int k,String fileUri) throws IOException {
		List<Point> pointList = readPoint(fileUri);
		
		Set<Integer> centerSet = new HashSet<Integer>();
		Random randomGenerator = new Random();
		while(centerSet.size()<k) {
			centerSet.add(randomGenerator.nextInt(pointList.size()));
		}
		
		List<Point> centerList = new ArrayList<Point>();
		int index = 0;
		for (Integer i : centerSet) {
			Point point = pointList.get(i);
			point.setId(index);
			centerList.add(pointList.get(i));
			index++;
		}
		
		return pointList;
	}
	
	public static List<Point> readPoint(String fileUri) throws IOException {
		List<Point> pointList = new ArrayList<Point>();
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fileUri), conf);
		Path path = new Path(fileUri);
		
		SequenceFile.Reader reader = null;
		

		Point point = new Point();
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			reader.next(new Text("point"), point);
			pointList.add(point);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			reader.close();
			fs.close();	
		}
		reader.close();
		fs.close();
		
		return pointList;
	}
	
	public static void main (String[] args) {
		try {
			List<Double> list1 = new ArrayList<Double>();
			VectorWritable vector1 = new VectorWritable(5);
			list1.add(2.0);
			list1.add(0.);
			list1.add(3.0);
			vector1.setData(list1);
			
			Point point = new Point();
			point.setData(vector1);
			point.setId(123);
			List<Point> centerList = new ArrayList<Point>();
			centerList.add(point);
			CenterUtils.writeCenter(centerList, "aa.seq");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
