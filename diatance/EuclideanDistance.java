package diatance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lib.Point;
import lib.VectorWritable;

public class EuclideanDistance implements Distance {

	@Override
	public double computeDistance(Point p1, Point p2) {
		return computeDistance(p1.getData(), p2.getData());
	}

	@Override
	public double computeDistance(VectorWritable vector1, VectorWritable vector2) {
		if (vector1.getColumnNum() != vector2.getColumnNum()) {
			return 0;
		}
		Map<Integer,Double> data1 = vector1.getData();
		Map<Integer,Double> data2 = vector2.getData();
		
		int maxColumnNum = vector1.getColumnNum();
		double result = 0;
		for (int i=0; i<maxColumnNum; i++) {
			double column1 = 0;
			if (data1.containsKey(i)) {
				column1 = data1.get(i);
			}
			
			double column2 = 0;
			if (data2.containsKey(i)) {
				column2 = data2.get(i);
			}
			
			result += (column2 -column1) * (column2 -column1); 
		}
		return result;
	}

	public static void main (String[] args) {
		List<Double> list1 = new ArrayList<Double>();
		VectorWritable vector1 = new VectorWritable(5);
		list1.add(1.1);
		list1.add(0.);
		list1.add(12.1);
		vector1.setData(list1);
		
		List<Double> list2 = new ArrayList<Double>();
		VectorWritable vector2 = new VectorWritable(5);
		list2.add(0.);
		list2.add(1.1);
		list2.add(12.1);
		vector2.setData(list2);
		
		System.out.println(new EuclideanDistance().computeDistance(vector1, vector2));
	}
}
