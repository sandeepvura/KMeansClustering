package edu.uic.ids561.aramna2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

public class DataPointRecordReader implements RecordReader<Text, DataPoint>{

	private LineRecordReader lineReader;
	private LongWritable lineKey;
	private Text lineValue;
	
	public DataPointRecordReader(JobConf job, FileSplit split) throws IOException {
		
		lineReader = new LineRecordReader(job, split);
		lineKey = lineReader.createKey();
		lineValue = lineReader.createValue();
	}
	
	@Override
	public boolean next(Text key, DataPoint value) throws IOException {
		
		// Get the next line
		if (!lineReader.next(lineKey, lineValue)) { 
			return false;
		}
		
		// parse the lineValue which is in the format: Data_ID<tab>X1,X2,X3,X4,X5,X6,X7,X8,X9,X10
		String [] pieces = lineValue.toString().split("	");
		
		if (pieces.length != 2) {
			throw new IOException("Invalid record received");
		}
		
		String[] valueSplit = pieces[1].split(",");
		
		if(valueSplit.length != 10){
			throw new IOException("Invalid record received");
		}

		double data[] = new double[10];
		
		String inputKey = null;

		try{
			// parsing the values from the input file
			inputKey = pieces[0].trim();
			
			// creating the data[] array which represents a 10 dimentional data point
			for(int i=0 ; i<10 ; i++){
				data[i] = Double.parseDouble(valueSplit[i]);
			}
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		// overwrite the output key and values and set appropriate value from the input file
		key.set(inputKey);
		
		value.data = data;
		
		return true;
	}
	
	@Override
	public void close() throws IOException {
		lineReader.close();
	}

	@Override
	public Text createKey() {
		return new Text();
	}

	@Override
	public DataPoint createValue() {
		return new DataPoint();
	}

	@Override
	public long getPos() throws IOException {
		return lineReader.getPos();
	}

	@Override
	public float getProgress() throws IOException {
		return lineReader.getProgress();
	}

}
