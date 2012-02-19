package util;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import lib.Point;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ReadUtils {
	
	public static void readClusterPoints(Configuration conf, String uri) {
		
		List<Point> clusterPoints = new ArrayList<Point>();
		SequenceFile.Reader reader = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader
					.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader
					.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
//				clusterPoints.add(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			IOUtils.closeStream(reader);
		}
		
	}

}
