package edu.uic.ids561.aramna2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This is the data structure to store Data Points. It will be in the format `key<tab>x1,x2,x3 ... x10`
 * @author aramna2
 */
public class DataPoint implements Writable{
	
	// This variable will store the data point x1,x2,x3 ... x10
	double[] data;
	
	// To store the number of items for which the sum was calculated. (Will be used in Combiner)
	long sumCount;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		for( int i=0 ; i<10 ; ++i ){
			data[i] = in.readDouble();
		}
		
		sumCount = in.readLong();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		
		for( int i=0 ; i<10 ; ++i ){
			out.writeDouble(data[i]);
		}
		
		out.writeLong(sumCount);
	}
	
	public DataPoint(double[] data, long sumCount) {
		super();
		this.data = data;
		this.sumCount = sumCount;
	}

	public DataPoint(){
		this.data = new double[10];
	}
	
	public String toString(){
		
		StringBuffer returnString = new StringBuffer();
		
		for( int i=0 ; i<10 ; ++i ){
			returnString.append(data[i] + ",");
		}
		
		// Removing the trailing `,`
		return returnString.toString().substring(0, returnString.toString().length() -1 );
	}
}
