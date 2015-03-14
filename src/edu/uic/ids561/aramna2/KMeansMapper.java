package edu.uic.ids561.aramna2;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class KMeansMapper extends MapReduceBase implements Mapper<Text, DataPoint, Text, DataPoint> {

	@Override
	public void map(Text key, DataPoint value,
			OutputCollector<Text, DataPoint> output, Reporter reporter)
			throws IOException {
			
		double shortestDistance = Double.MAX_VALUE;
		double euclideanDistance;
		String centroidAssociatedWithShortedDistance = null;
		
		/*Find Euclidean distance of the `DataPoint` with all centroids and associate the `DataPoint`
		with the centroid which has the least distance to it*/
		
		Iterator<Entry<String, DataPoint>> mapIterator = KMeans.centroidMap.entrySet().iterator();
		
		while(mapIterator.hasNext()){

			Map.Entry<String, DataPoint> centroid = (Map.Entry<String, DataPoint>) mapIterator.next();
			euclideanDistance = EuclideanDistanceUtils.computeEuclideanDistance(centroid.getValue(), value);
			
			if(euclideanDistance < shortestDistance){
				shortestDistance = euclideanDistance;
				centroidAssociatedWithShortedDistance = centroid.getKey();
			}
		}
		
		output.collect(new Text(centroidAssociatedWithShortedDistance), value);
	}
}
