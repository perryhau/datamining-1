package unitTest;

import java.util.ArrayList;
import java.util.List;

import lib.Point;
import lib.VectorWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import preData.CreatePoint;


public class CreatePointTest {
	
	private CreatePoint.MyMapper mapper;
	private CreatePoint.MyReducer reducer;
	private MapReduceDriver<Text, Text, Text, Text,Text,Point> mapreduceDriver;
	private MapDriver<Text, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Point> reduceDriver;
	

	@Before
	public void setUp() {
		mapper = new CreatePoint.MyMapper();
		reducer = new CreatePoint.MyReducer();
		mapreduceDriver = new MapReduceDriver<Text, Text, Text, Text,Text,Point>();
		mapDriver = new MapDriver<Text, Text, Text, Text>();
		reduceDriver = new ReduceDriver<Text, Text, Text, Point>();
		mapreduceDriver.setMapper(mapper);
		mapreduceDriver.setReducer(reducer);
		mapDriver.setMapper(mapper);
		reduceDriver.setReducer(reducer);

	}

	@Test
	public void testReduce() {
		List<Double> list1 = new ArrayList<Double>();
		VectorWritable vector1 = new VectorWritable(5);
		list1.add(2.0);
		list1.add(0.);
		list1.add(3.0);
		vector1.setData(list1);
		
		Point point = new Point();
		point.setData(vector1);
		point.setId(123);
		
		List<Text> list = new ArrayList<Text>();
		list.add(new Text("1"));
		list.add(new Text("1"));
		list.add(new Text("1"));
		list.add(new Text("3"));
		list.add(new Text("3"));
		reduceDriver.setInput(new Text("123"), list);
		Configuration conf = new Configuration();
		conf.set("nidFile", "nidFile.txt");
		reduceDriver.setConfiguration(conf);
		reduceDriver.addOutput(new Text("123"), point);
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapreduceDriver.addInput(new Text("1136659"), new Text("9991157,2011-12-11:10:15:11"));
		mapreduceDriver.addInput(new Text("1136659"), new Text("9991157,2011-12-11:10:14:11"));
		mapreduceDriver.addInput(new Text("1145866"), new Text("9991157,2011-12-11:10:01:11"));
		mapreduceDriver.addInput(new Text("1153533"), new Text("9991157,2011-12-11:11:24:11"));
		mapreduceDriver.addInput(new Text("1153533"), new Text("9991157,2011-12-11:11:25:11"));
		
		List<Double> list1 = new ArrayList<Double>();
		VectorWritable vector1 = new VectorWritable();
		list1.add(2.0);
		list1.add(2.);
		list1.add(1.0);
		vector1.setData(list1);
		
		Point point = new Point();
		point.setData(vector1);
		point.setId(9991157);
		
		Configuration conf = new Configuration();
		conf.set("nidFile", "nidFile2.txt");
		mapreduceDriver.setConfiguration(conf);
		mapreduceDriver.addOutput(new Text("9991157"), point);
		
		mapreduceDriver.runTest();
	}
	
}
