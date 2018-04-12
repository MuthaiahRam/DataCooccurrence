package com.cs267.lab3.part2;

/**
 * @author Muthiaah
 * Triplet - Bean class to wrap a triplet
 */
class Triplet implements Comparable<Triplet> {
	
			int count;
           
            String triplet;
            String pair;

            /**
             * Constructor
             * @param triplet
             * @param pair
             * @param count
             */
            Triplet(String triplet, String pair, int count) {
                
                this.triplet = triplet;
                this.pair = pair;
                this.count=count;
            }

			/* (non-Javadoc)
			 * @see java.lang.Comparable#compareTo(java.lang.Object)
			 */
			@Override
			public int compareTo(Triplet o) {
				// TODO Auto-generated method stub
				return 0;
			}

           
        }