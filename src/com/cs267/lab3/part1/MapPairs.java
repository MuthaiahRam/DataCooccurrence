package com.cs267.lab3.part1;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @author Muthaiah
 * MapPairs - Class to map <Pair, count> . Count =1
 */
public class MapPairs extends Mapper<LongWritable, Text, Pair, Text> {
	
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	System.err.println("inside map");
            String[] words = value.toString().split(" ");
            //Two loops to generate all possible pairs
            for (String word : words) {
            	//generalized to work for mail (Question 2) as well
                if (word.matches("^\\w+$")) {
                    int count = 0;
                    for (String term : words) {
                        if (term.matches("^\\w+$") && !term.equals(word)) {
                            //context.write(new Text(word + "," + term), new Text("1"));
                            Pair pair=new Pair(word,term);
                            System.err.println("mapper"+pair.getKey()+":"+pair.getValue());
                            context.write(pair, new Text("1"));//emit count as 1
                            count++;
                        }
                    }
                    Pair pair=new Pair(word,"*");// Emit total count <Pair,*>
                    System.err.println("mapper"+pair.getKey()+":"+pair.getValue());
                   
                    context.write(pair, new Text(String.valueOf(count)));
                    //context.write(new Text(word + ",*"), new Text(String.valueOf(count)));
                }
            }
        }
    }
