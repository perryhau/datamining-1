package lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point implements Writable, Cloneable {

	@Override
	protected Object clone() throws CloneNotSupportedException {

		Point point = (Point) super.clone();
		point.data = (VectorWritable) data.clone();
		
		return point;
	}
	
	private int id = 0;
	private VectorWritable data = new VectorWritable();
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public VectorWritable getData() {
		return data;
	}
	public void setData(VectorWritable data) {
		this.data = data;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		data =  new VectorWritable();
		data.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		data.write(out);
		
	}
	@Override
	public boolean equals(Object obj) {

		if (obj.hashCode() == this.hashCode())
			return true;
		else
			return false;
	}
	@Override
	public int hashCode() {

		return id+data.hashCode();
	}
	@Override
	public String toString() {

		return id+"--"+data.toString();
	}
	
	

}
