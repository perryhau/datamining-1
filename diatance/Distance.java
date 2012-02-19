package diatance;

import lib.Point;
import lib.VectorWritable;

public interface Distance {

	public double computeDistance(Point p1, Point p2);
	
	public double computeDistance(VectorWritable vector1, VectorWritable vector2);
	
}
