package com.cs267.lab3.part1;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Muthaiah
 *
 */
public class ReduceStripes extends Reducer<Text, Text, Text, Text> {
	//TreeSet<Pair> priorityQueue = new TreeSet<>();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		java.util.Map<String, Integer> stripe = new HashMap<>();
		double totalCount = 0;
		//String keyStr = key.toString();
		int count=0;
		// 0,1	14500:1,3000:1,
		for (Text value : values) {
			
			String[] stripes = value.toString().split(",");

			for (String termCountStr : stripes) {
				String[] termCount = termCountStr.split(":");
				String term = termCount[0];
				count = Integer.parseInt(termCount[1]);

				Integer countSum = stripe.get(term);
				stripe.put(term, (countSum == null ? 0 : countSum) + count);

				totalCount += count;
			}
		}

		for (java.util.Map.Entry<String, Integer> entry : stripe.entrySet()) {
			
			context.write(new Text("["+key+","+entry.getKey()+"]"+":"+count), new Text(String.valueOf(entry.getValue() / totalCount)));

		}
	}

	/*protected void cleanup(Context context) throws IOException,
			InterruptedException {
		while (!priorityQueue.isEmpty()) {
			Pair pair = priorityQueue.pollLast();
		//	context.write(new Text("["+pair.key+","+pair.value+"]"+":"+pair.count), new Text(String.valueOf(pair.relativeFrequency)));
		}
	}*/

}
