package edu.uic.ids561.aramna2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class KMeans {

	public static Map<String, DataPoint> centroidMap = new HashMap<String, DataPoint>();
	public static Map<String, DataPoint> centroidMapNext = new HashMap<String, DataPoint>();
	
	public static void main(String[] args) {
		
		if(args.length != 3){
			System.out.println("Error: expecting 3 command line arguments");
			System.exit(1);
		}
		
		// Before starting the KMeans Map-Reduce job, store centroids from the file to Global HashMap `centroidMap`
		try {
			new ParseCentroid().parseCentroid(args[2]);
			
			if(centroidMap.size() < 3 || centroidMap.size() > 8){
				
				System.out.println("K should be a value between 3 and 8");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(edu.uic.ids561.aramna2.KMeans.class);

		int iterationCount = 0;
		
		boolean isTerminateProgram = false;
		
		while( !isTerminateProgram ){
		
			// Setting centroidMap to centroid recalculated in previous iteration
			if( 0 != iterationCount ){
				Iterator<Entry<String, DataPoint>> iterator = centroidMap.entrySet().iterator();
				while(iterator.hasNext()){
					Map.Entry<String, DataPoint> pair = (Map.Entry<String, DataPoint>) iterator.next();
					centroidMap.put(pair.getKey(),centroidMapNext.get(pair.getKey()));
				}
			}

			// Input format
			conf.setInputFormat(DataPointInputFormat.class);
			
			// specify output types
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(DataPoint.class);
	
			String input, output;
			
			input = args[0];
			output = args[1];
			
			output = output + (iterationCount+1);
			
			// specify input and output DIRECTORIES (not files)
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			// specify a mapper
			conf.setMapperClass(KMeansMapper.class);
			conf.setNumMapTasks(3);
	
			// specify a combiner
			conf.setCombinerClass(KMeansCombiner.class);
			
			// specify a reducer and set number of reducers = K
			conf.setReducerClass(KMeansReducer.class);
			conf.setNumReduceTasks( centroidMap.size() );
			
			// specify partitioner
			conf.setPartitionerClass(KMeansPartitioner.class);
			
			client.setConf(conf);
			try {
				JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}

			/* Checking if current centroid converges with previous centroid */
			Iterator<Entry<String, DataPoint>> mapIterator = centroidMap.entrySet().iterator();
			
			isTerminateProgram = true;
			
			while(mapIterator.hasNext()){
				
				Map.Entry<String, DataPoint> pair = (Map.Entry<String, DataPoint>) mapIterator.next();
				if( EuclideanDistanceUtils.computeSumOfSquares(pair.getValue(), centroidMapNext.get(pair.getKey())) > .01 ){
					isTerminateProgram = false;
				}
			}
			/* --- */
			
			iterationCount ++;
		}
	}

}
