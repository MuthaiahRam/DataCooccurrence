package com.cs267.lab3.part2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Muthaiah
 *
 */
public class ReduceTripletStripes extends Reducer<Text, Text, Text, Text> {
	HashMap<String, Integer> hashMap1;
	HashMap<String, Integer> hashMap2;

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Text value = values.iterator().next();

		int count = 0;
		String term = "";
		// combinertriplet0,1::14500:1,
		// combinertriplet0,1::3000:1,
		for (Text value : values) {

			String[] stripes = value.toString().split(",");

			String[] termCount = stripes[0].split(":");
			term = termCount[0];
			count = Integer.parseInt(termCount[1]);

			String keyString = "[" + key.toString() + "," + term.trim() + "]";// triplet
			System.err.println("triplet" + keyString);
			keyString = "["
					+ keyString.substring(keyString.indexOf(",") + 1,
							keyString.length()).trim(); // pair
			System.err.println("pair" + keyString);
			int keyInteger = Integer.valueOf(keyString.substring(
					keyString.indexOf("[") + 1, keyString.indexOf(","))); // partition
																			// identify

			if (keyInteger < 8000) {

				context.write(
						new Text("[" + key.toString() + "," + term.trim() + "]"),
						new Text(String.valueOf(count)
								+ ":"
								+ String.valueOf(count
										/ hashMap1.get(keyString))));

			} else {
				context.write(
						new Text("[" + key.toString() + "," + term.trim() + "]"),
						new Text(String.valueOf(count)
								+ ":"
								+ String.valueOf(count
										/ hashMap2.get(keyString))));

			}
		}
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {
		if (hashMap1 == null)
			hashMap1 = readFile("part-r-00000");
		if (hashMap2 == null)
			hashMap2 = readFile("part-r-00001");

		/*
		 * while (!priorityQueue1.isEmpty()) { Triplet triplet =
		 * priorityQueue1.pollLast(); int countPairs =
		 * hashMap1.get(triplet.pair);
		 * 
		 * context.write( new Text(triplet.triplet), new
		 * Text(String.valueOf(triplet.count) + ":" +
		 * String.valueOf(triplet.count / countPairs))); } while
		 * (!priorityQueue2.isEmpty()) { Triplet triplet =
		 * priorityQueue2.pollLast(); int countPairs =
		 * hashMap2.get(triplet.pair);
		 * 
		 * context.write( new Text(triplet.triplet), new
		 * Text(String.valueOf(triplet.count) + ":" +
		 * String.valueOf(triplet.count / countPairs))); }
		 */

	}

	public HashMap<String, Integer> readFile(String name) {
		Path pt = new Path("hdfs:/user/cloudera/lab3/part/output/pairs/"
				+ name);// Location
		// of
		// file
		// in
		// HDFS
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

		try {
			FileSystem fs = FileSystem.get(new Configuration());

			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();

			while (line != null) {

				//[0,4012]:1	0.14285714285714285
				String array[] = line.split("\t");
				String number = array[0].substring(array[0].indexOf(":")+1,array[0].length());
				hashMap.put(array[0].substring(0,array[0].indexOf(":")),
						Integer.valueOf(number));
				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return hashMap;
	}

}
