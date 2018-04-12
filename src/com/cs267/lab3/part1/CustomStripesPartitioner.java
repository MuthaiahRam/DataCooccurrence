package com.cs267.lab3.part1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Muthaiah CustomStripesPartitioner - Class to Class to direct Key from
 *         mapper to corresponding reducer
 */
public class CustomStripesPartitioner extends Partitioner<Text, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(Text arg0, Text arg1, int arg2) {
		// Text object should be sorted - implemented compateTo() method in Pair
		// class
		// Inverted secondary sorting - Reference from Jimmy's book.
		// Two partition = Two reducer
		int value = Integer.valueOf(String.valueOf(arg0).split(",")[0].replace(
				"[", "").trim());

		if (value < 8000) {
			return 0;
		}

		return 1;

	}

}
