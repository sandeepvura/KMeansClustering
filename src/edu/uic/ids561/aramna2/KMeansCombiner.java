package edu.uic.ids561.aramna2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KMeansCombiner extends MapReduceBase implements Reducer<Text, Iterator<DataPoint>, Text, DataPoint> {

	@Override
	public void reduce(Text key, Iterator<Iterator<DataPoint>> values,
			OutputCollector<Text, DataPoint> output, Reporter reporter)
			throws IOException {
		
		double[] newCentroid = new double[10];
		
		int numOfValues = 0;
		
		while(values.hasNext()){
			DataPoint dataPoint = (DataPoint) values.next();
			
			// Calculate the sum of the data points.
			for(int i=0 ; i<10 ; i++ ){
				newCentroid[i] += dataPoint.data[i];
			}
			
			// Increment number of values, which will later be stored in `sumCount` variable in DataPoint class
			numOfValues ++;
		}
		
		output.collect(key, new DataPoint(newCentroid, numOfValues));
	}

}
