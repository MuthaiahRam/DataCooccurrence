package com.cs267.lab3.part2;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Muthaiah
 * CombineTripletStripes- Class to combine all triplet pairs 
 *
 */
public  class CombineTripletStripes extends Reducer<Text, Text, Text, Text> {
    @SuppressWarnings("rawtypes")
	public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        java.util.Map<String, Integer> stripe = new HashMap<>();
       // 0,1	14500:1,3000:1,
        for (Text value : values) {
            String[] stripes = value.toString().split(",");
            String term="";int count=0;
            for (String termCountStr : stripes) {
                String[] termCount = termCountStr.split(":");
                 term = termCount[0];//3rd
                 count = Integer.parseInt(termCount[1]);
                 
                Integer countSum = stripe.get(term);
                stripe.put(term, (countSum == null ? 0 : countSum) + count);
               // context.write(key, new Text(term+":"+stripe.get(term)+","));
            }
            
        }

        //StringBuilder stripeStr = new StringBuilder();
        for (java.util.Map.Entry entry : stripe.entrySet()) {
            //stripeStr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
        	System.err.println("combinertriplet"+key+"::"+entry.getKey()+":"+entry.getValue()+",");
        	context.write(key, new Text(entry.getKey()+":"+entry.getValue()+","));
        }

        //context.write(key, new Text(stripeStr.toString()));
    }
}
