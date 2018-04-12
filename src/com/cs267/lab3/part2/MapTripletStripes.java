package com.cs267.lab3.part2;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Muthaiah
 *
 */
public class MapTripletStripes extends Mapper<LongWritable, Text, Text, Text> {
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");

		for (String word : words) {
			if (word.matches("^\\w+$")) {

				for (String term : words) {
				
					java.util.Map<String, Integer> stripe = new HashMap<>();
					for (String item : words) {
						
						if ((!item.equals(term) && !item.equals(word)&& !word.equals(term))) {
							System.err.println("******"+word+":"+term+":"+item);
							Integer count = stripe.get(item);
							stripe.put(item, (count == null ? 0 : count) + 1);
						}
					}
					
					StringBuilder stripeStr = new StringBuilder();
					for (@SuppressWarnings("rawtypes")
					java.util.Map.Entry entry : stripe.entrySet()) {
						stripeStr.append(entry.getKey()).append(":")
								.append(entry.getValue()).append(",");
					}

					if (!stripe.isEmpty()) {
						
						context.write(new Text(word + "," + term), new Text(
								stripeStr.toString()));
					}
					}
				}
			}
		}
	}

