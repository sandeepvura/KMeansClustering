package edu.uic.ids561.aramna2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class KMeansPartitioner implements Partitioner<Text, DataPoint> {

	@Override
	public void configure(JobConf arg0) {
		
	}

	@Override
	public int getPartition(Text key, DataPoint value, int numOfReduceTasks) {

		if(numOfReduceTasks == 0){
			return 0;
		}
		else{
			// this will map the intermediate key-value pairs among the reducers
			if( key.toString().contains("1") ){
				return 0;
			}
			else if( key.toString().contains("2") ){
				return 1 % numOfReduceTasks;
			}
			else if( key.toString().contains("3") ){
				return 2 % numOfReduceTasks;
			}
			else if( key.toString().contains("4") ){
				return 0;
			}
			else if( key.toString().contains("5") ){
				return 1 % numOfReduceTasks;
			}
			else if( key.toString().contains("6") ){
				return 2 % numOfReduceTasks;
			}
			else if( key.toString().contains("7") ){
				return 0;
			}
			else if( key.toString().contains("8") ){
				return 1 % numOfReduceTasks;
			}
			else{
				return 0;
			}
		}
	}
}
