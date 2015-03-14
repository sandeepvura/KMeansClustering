package edu.uic.ids561.aramna2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KMeansReducer extends MapReduceBase implements
		Reducer<Text, Iterator<DataPoint>, Text, DataPoint> {

	@Override
	public void reduce(Text key, Iterator<Iterator<DataPoint>> values,
			OutputCollector<Text, DataPoint> output, Reporter reporter)
			throws IOException {

		double[] newCentroid = new double[10];

		int numOfValues = 0;

		// Calculate the sum of the values
		while (values.hasNext()) {
			DataPoint dataPoint = (DataPoint) values.next();

			for (int i = 0; i < 10; i++) {
				newCentroid[i] += dataPoint.data[i];
			}

			// Calculate the number of values per key (centroid)
			numOfValues += dataPoint.sumCount;
			
		}

		// Divide the sum by `numOfValues` to get the average.
		for (int i = 0; i < 10; i++) {
			newCentroid[i] = newCentroid[i] / numOfValues;
		}

		KMeans.centroidMapNext.put(key.toString(), new DataPoint(newCentroid, 0));
		output.collect(key, new DataPoint(newCentroid ,0));
	}
}
