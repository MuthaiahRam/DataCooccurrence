package com.cs267.lab3.part2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Muthaiah
 *
 */
public class MapTripletPairs extends Mapper<LongWritable, Text, Text, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");

		for (int i = 0; i < words.length; i++) {
			for (int j = 0; j < words.length; j++) {
				for (int k = 0; k < words.length; k++) {

					if (!words[i].equals(words[j])
							&& !words[i].equals(words[k])
							&& !words[j].equals(words[k])) {
						context.write(new Text("["+words[i] + "," + words[j] + ","
								+ words[k]+"]"), new Text("1"));

					}
				}

			}
		}
	}
}
