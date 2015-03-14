package edu.uic.ids561.aramna2;

public class EuclideanDistanceUtils {

	public static double computeEuclideanDistance(DataPoint d1, DataPoint d2){
		
		// Calculating the Euclidean distance
		double distance = Math.sqrt(
							 ( (d1.data[0] - d2.data[0])*(d1.data[0] - d2.data[0]) ) 
							+( (d1.data[1] - d2.data[1])*(d1.data[1] - d2.data[1]) )
							+( (d1.data[2] - d2.data[2])*(d1.data[2] - d2.data[2]) )
							+( (d1.data[3] - d2.data[3])*(d1.data[3] - d2.data[3]) )
							+( (d1.data[4] - d2.data[4])*(d1.data[4] - d2.data[4]) )
							+( (d1.data[5] - d2.data[5])*(d1.data[5] - d2.data[5]) )
							+( (d1.data[6] - d2.data[6])*(d1.data[6] - d2.data[6]) )
							+( (d1.data[7] - d2.data[7])*(d1.data[7] - d2.data[7]) )
							+( (d1.data[8] - d2.data[8])*(d1.data[8] - d2.data[8]) )
							+( (d1.data[9] - d2.data[9])*(d1.data[9] - d2.data[9]) )
						);
		
		return distance;
	}
	
public static double computeSumOfSquares(DataPoint d1, DataPoint d2){
		
		// Calculating the Euclidean distance
		double distance =  ( (d1.data[0] - d2.data[0])*(d1.data[0] - d2.data[0]) ) 
							+( (d1.data[1] - d2.data[1])*(d1.data[1] - d2.data[1]) )
							+( (d1.data[2] - d2.data[2])*(d1.data[2] - d2.data[2]) )
							+( (d1.data[3] - d2.data[3])*(d1.data[3] - d2.data[3]) )
							+( (d1.data[4] - d2.data[4])*(d1.data[4] - d2.data[4]) )
							+( (d1.data[5] - d2.data[5])*(d1.data[5] - d2.data[5]) )
							+( (d1.data[6] - d2.data[6])*(d1.data[6] - d2.data[6]) )
							+( (d1.data[7] - d2.data[7])*(d1.data[7] - d2.data[7]) )
							+( (d1.data[8] - d2.data[8])*(d1.data[8] - d2.data[8]) )
							+( (d1.data[9] - d2.data[9])*(d1.data[9] - d2.data[9]) );
		
		return distance;
	}
}
