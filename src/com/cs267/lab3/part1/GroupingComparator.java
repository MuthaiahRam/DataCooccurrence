package com.cs267.lab3.part1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Muthaiah
 * GroupingComparator - Class to group keys - Inverted sorting concept <Key, *> should go before the <key,count>
 */
public class GroupingComparator extends WritableComparator{
	
	/**
	 * Constructor
	 */
	public GroupingComparator(){
		super(Pair.class,true);
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 * Method to compare two pairs
	 */
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w,@SuppressWarnings("rawtypes") WritableComparable w1){
		Pair pair=(Pair) w;
		Pair pair1=(Pair) w1;
		System.err.println("compartor"+pair.getKey()+":"+pair.getValue());
		int compareValue=pair.getKey().compareTo(pair1.getKey());
		if(compareValue==0){
		compareValue= pair.getValue().compareTo(pair1.getValue());
		}
		return compareValue;
	}
}
