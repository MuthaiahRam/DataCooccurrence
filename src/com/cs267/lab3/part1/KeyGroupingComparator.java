package com.cs267.lab3.part1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Muthaiah
 * KeyGroupingComparator- Class to group all keys to go to same reducer along with <key,*>
 */
public class KeyGroupingComparator extends WritableComparator{
	
	/**
	 * Constructor
	 */
	public KeyGroupingComparator(){
		super(Pair.class,true);
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w,@SuppressWarnings("rawtypes") WritableComparable w1){
		Pair pair=(Pair) w;
		Pair pair1=(Pair) w1;
		System.err.println("compartor"+pair.getKey()+":"+pair.getValue());
		return Pair.compareKey(pair, pair1);
	}
}
