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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import diatance.EuclideanDistance;

public class CenterUtils {
	private Configuration conf;

	public CenterUtils(Configuration conf) {
		this.conf = conf;
	}

	public void createRandomCenter(int k, String pointFile, String outputFile) {
		List<Point> centerList;
		try {
			centerList = randomInitialCenter(k, pointFile);
			System.out.println("centers:");
			for (Point point : centerList) {
				System.out.println(point);
			}
			writeCenter(centerList, outputFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public double compareCentorid(List<Point> centerList1,
			List<Point> centerList2) {
		if (centerList1.size() != centerList2.size()) {
			return -1.;
		}
		int size = centerList1.size()>centerList2.size()?centerList2.size():centerList1.size();
		double var = 0;
		EuclideanDistance distance = new EuclideanDistance();
		for (int i = 0; i < size; i++) {
			if (centerList1.get(i) == null || centerList2.get(i) == null) {
				return var;
			}
			var += distance.computeDistance(centerList1.get(i),
					centerList2.get(i));
		}

		return var;
	}

	public void writeCenter(List<Point> centerList, String outputFile)
			throws IOException {

		FileSystem fs = FileSystem.get(URI.create(outputFile), conf);

		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
				new Path(outputFile), IntWritable.class, Point.class);
		for (Point center : centerList) {
			writer.append(new IntWritable(center.getId()), center);
		}
		writer.close();
		IOUtils.closeStream(writer);

	}

	public List<Point> randomInitialCenter(int k, String fileUri)
			throws IOException {
		List<Point> pointList = readPoint(fileUri);

		Set<Integer> centerSet = new HashSet<Integer>();
		Random randomGenerator = new Random();
		while (centerSet.size() < k) {
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

		return centerList;
	}

	public List<Point> readPointFile(String fileUri) throws IOException {
		List<Point> pointList = new ArrayList<Point>();

		FileSystem fs = FileSystem.get(URI.create(fileUri), conf);
		Path path = new Path(fileUri);

		SequenceFile.Reader reader = null;

		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			IntWritable key = new IntWritable();
			Point point = new Point();
			while (reader.next(key, point)) {
				pointList.add((Point) point.clone());
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
			// reader.close();
			// fs.close();
		}
		// reader.close();
		// fs.close();

		return pointList;
	}

	public List<Point> readPoint(String fileUri) throws IOException {

		FileSystem fs = FileSystem.get(URI.create(fileUri), conf);
		Path path = new Path(fileUri);
		FileStatus fileStatus = fs.getFileStatus(path);
		if (fileStatus.isDir()) {
			FileStatus[] fileStatusList = fs.listStatus(path);
			List<Point> pointsList = new ArrayList<Point>();
			for (FileStatus fileStatus2 : fileStatusList) {
				if (fileStatus2.getPath().getName().matches(".*part-r.*")) {
					pointsList.addAll(readPointFile(fileStatus2.getPath()
							.toString()));
				}
			}

			return pointsList;
		} else {
			return readPointFile(fileUri);
		}

	}

	public static void main(String[] args) {
		List<String> a = new ArrayList<String>();
		a.add("a");
		a.add("b");
		a.add(null);
		
		List<String> b = new ArrayList<String>();
		b.add("c");
		b.add("c");
		
		if (a.size() == b.size()) {
			System.out.println("ok");
		}
	}
}
