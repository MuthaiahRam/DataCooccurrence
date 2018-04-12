package com.cs267.lab3.part1;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @author Muthaiah
 * CombinePairs - Class to combine the count of all the pairs
 */
public class CombinePairs extends Reducer<Pair, Text, Pair, Text> {
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        public void reduce(Pair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
          //Log written to map task - cloudera syserr logs
            System.err.println("combiner+++++"+key.getKey()+":"+key.getValue());
            //Emit <Pair,total count> to context
            context.write(key, new Text(String.valueOf(count)));
            
        }
    }
