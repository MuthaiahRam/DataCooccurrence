package com.cs267.lab3.part1;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Muthaiah
 * CombineStripes- Class to combine the count of all the stripes
 */
public  class CombineStripes extends Reducer<Text, Text, Text, Text> {
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @SuppressWarnings("rawtypes")
	public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
    	/*Utility Map to store the stripe of a key*/
        java.util.Map<String, Integer> stripe = new HashMap<>();
        
/*Every pair is separated by a ',' and key value is separated by a ':'*/
        for (Text value : values) {
            String[] stripes = value.toString().split(",");

            for (String termCountStr : stripes) {
                String[] termCount = termCountStr.split(":");
                String term = termCount[0];
                int count = Integer.parseInt(termCount[1]);
                /* Get the previous count for the same key*/
                Integer countSum = stripe.get(term);
                stripe.put(term, (countSum == null ? 0 : countSum) + count);
            }
        }

        StringBuilder stripeStr = new StringBuilder();
        //Emit data from stripe as a text 
        for (java.util.Map.Entry entry : stripe.entrySet()) {
            stripeStr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        }

        context.write(key, new Text(stripeStr.toString()));
    }
}
