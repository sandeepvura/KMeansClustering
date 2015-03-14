package edu.uic.ids561.aramna2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class DataPointInputFormat extends FileInputFormat<Text, DataPoint>{

	@Override
	public RecordReader<Text, DataPoint> getRecordReader(InputSplit arg0,
			JobConf arg1, Reporter arg2) throws IOException {

		return new DataPointRecordReader(arg1, (FileSplit) arg0);
	}

	public RecordReader<Text, DataPoint> createRecordReader( InputSplit input, JobConf job,
			Reporter reporter) throws IOException { 
			reporter.setStatus(input.toString());
			return new DataPointRecordReader(job, (FileSplit) input); 
	}
		
}
