package com.cs267.lab3.part1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Muthaiah
 * CustomPartitioner- Class to direct Key from mapper to corresponding reducer
 */
public class CustomPartitioner extends Partitioner<Pair, Text> {

	@Override
	public int getPartition(Pair arg0, Text arg1, int arg2) {
		//Pair object should be sorted - implemented compateTo() method in Pair class
		//Inverted secondary sorting - Reference from Jimmy's book.
		// Two partition = Two reducer 
		int value=Integer.valueOf(arg0.getKey());
		
		 if (value < 8000) {
			return 0;
		} 

		return 1;

	}

}
