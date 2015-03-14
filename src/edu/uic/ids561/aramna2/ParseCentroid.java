package edu.uic.ids561.aramna2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class ParseCentroid {

	public void parseCentroid(String centroidFileDir) throws Exception {

		FileInputStream fis = null;
		BufferedReader reader = null;

		try {

			// read the file
			fis = new FileInputStream(centroidFileDir + "/centroid.txt");
			reader = new BufferedReader(new InputStreamReader(fis));

			String line, key, value[];
			DataPoint dataPoint;

			while (true) {

				line = reader.readLine();

				// read till end of file
				if (line == null) {
					break;
				}
				
				// parse the line into a `Datapoint` object and insert it in global Centroid HashMap
				if(line.split("\t").length != 2 || 
						line.split("\t")[1].split(",").length != 10 ){
					throw new Exception("Invalid data in centroid file");
				}
				
				key = line.split("\t")[0];
				value = line.split("\t")[1].split(",");
				
				dataPoint = new DataPoint();
				
				for( int i=0 ; i<value.length ; i++ ){
					dataPoint.data[i] = Double.parseDouble(value[i]);
				}
				
				// update centroidMap with the centroids in file
				KMeans.centroidMap.put(key, dataPoint);
			}
		} 
		
		catch (Exception ex) {
			throw ex;

		} finally {
			reader.close();
			fis.close();
		}
	}
}
