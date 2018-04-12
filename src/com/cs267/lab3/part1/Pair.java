package com.cs267.lab3.part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @author Muthaiah
 * Bean class - Pair to serialize pair objects
 */
class Pair implements Writable,WritableComparable<Pair> {
	
			
            String key;
            String value;
            /**
             * Default constructor
             */
            Pair(){
            	this.key=new String();
            	this.value=new String();
            }
            /**
             * @param key
             * @param value
             * Constructor 
             */
            Pair( String key, String value) {
              
                this.key = key;
                this.value = value;
               
            }
           

            /* (non-Javadoc)
             * @see java.lang.Comparable#compareTo(java.lang.Object)
             * Compare to method to check to pair objects
             */
            @Override
            public int compareTo(Pair pair) {
                //return this.getValue().compareTo(pair.getValue());
            	int compareValue=this.getKey().compareTo(pair.getKey());
            	 if(compareValue==0){
            		 compareValue=this.getValue().compareTo(pair.getValue());
            	 }
            	return compareValue;
                   
            }
            /**
             * @param pair
             * @param pair1
             * @return
             * Compare two keys of pair object - used to send key to corresponding reducer
             */
            public static int compareKey(Pair pair,Pair pair1) {
                return pair.getKey().compareTo(pair1.getKey());
                   
            }

			/* (non-Javadoc)
			 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
			 */
			@Override
			public void readFields(DataInput arg0) throws IOException {
				key=WritableUtils.readString(arg0);
				value=WritableUtils.readString(arg0);
				
			}

			/* (non-Javadoc)
			 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
			 */
			@Override
			public void write(DataOutput arg0) throws IOException {
				WritableUtils.writeString(arg0,key);
				WritableUtils.writeString(arg0,value);
				
			}
		
			/**
			 * @param arg0
			 * @return
			 * @throws IOException
			 */
			public static Pair read(DataInput arg0) throws IOException {
				Pair pair=new Pair();
				pair.readFields(arg0);
		        return pair;
				
			}
			
			/* (non-Javadoc)
			 * @see java.lang.Object#hashCode()
			 * Hashcode to determine the reducer
			 */
			@Override
			public int hashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + ((key == null) ? 0 : key.hashCode());
				result = prime * result
						+ ((value == null) ? 0 : value.hashCode());
				return result;
			}
			/* (non-Javadoc)
			 * @see java.lang.Object#equals(java.lang.Object)
			 */
			@Override
			public boolean equals(Object obj) {
				if (this == obj)
					return true;
				if (obj == null)
					return false;
				if (getClass() != obj.getClass())
					return false;
				Pair other = (Pair) obj;
				if (key == null) {
					if (other.key != null)
						return false;
				} else if (!key.equals(other.key))
					return false;
				if (value == null) {
					if (other.value != null)
						return false;
				} else if (!value.equals(other.value))
					return false;
				return true;
			}
			
			//Getter setter for the attributes
			@Override
		    public String toString() {
		        return "["+key+","+value+"]";
		    }
			public String getKey() {
				return key;
			}
			public void setKey(String key) {
				this.key = key;
			}
			public String getValue() {
				return value;
			}
			public void setValue(String value) {
				this.value = value;
			}
        }