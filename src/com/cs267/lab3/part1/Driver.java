package com.cs267.lab3.part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cs267.lab3.part2.CombineTripletPairs;
import com.cs267.lab3.part2.CombineTripletStripes;
import com.cs267.lab3.part2.MapTripletPairs;
import com.cs267.lab3.part2.MapTripletStripes;
import com.cs267.lab3.part2.ReduceTripletPairs;
import com.cs267.lab3.part2.ReduceTripletStripes;

/**
 * @author Muthaiah Driver class for to calculate conditional probability -
 *         Pairs and stripes
 *
 */
public class Driver extends Configured implements Tool {

	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {

		Configuration configuration1 = new Configuration();
		/* set # of splits for the mapper */
		configuration1.setLong(FileInputFormat.SPLIT_MAXSIZE, (long) 1048576);
		Job job = new Job(configuration1);
		job.setJarByClass(Driver.class);// Set the driver class in job
		job.setNumReduceTasks(2); // Set the # of reducer tasks
		job.setMapOutputKeyClass(Pair.class);//Map emits Pair object as key - toString() method overridden in Pair class
		job.setMapOutputValueClass(Text.class);//Map emits Text class as value
		job.setOutputKeyClass(Pair.class); //Reducer emits Pair object as key
		job.setOutputValueClass(Text.class); //Reducer emits Text - count as value
		job.setMapperClass(MapPairs.class); //Mapper class to emit count of every pair as 1
		job.setCombinerClass(CombinePairs.class); //Combiner class to calculate the sum of pairs
		job.setReducerClass(ReducePairs.class); //Reducer class to find conditional probability
		job.setPartitionerClass(CustomPartitioner.class); //Partitioner class to find secondary sort
		job.setInputFormatClass(TextInputFormat.class); //Input format for Mapper class 
		job.setOutputFormatClass(TextOutputFormat.class); //Output format for mapper class
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/pairs")); //Place the output file in this path
		job.waitForCompletion(true);

		// Job 2 - calculate conditional probability for a <pair> using stripes
		Configuration configuration = new Configuration();
		configuration.setLong(FileInputFormat.SPLIT_MAXSIZE, (long) 1048576);
		Job job1 = new Job(configuration);
		job1.setJarByClass(Driver.class);

		job1.setNumReduceTasks(2);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setMapperClass(MapStripes.class);
		job1.setCombinerClass(CombineStripes.class);
		job1.setReducerClass(ReduceStripes.class);
		job1.setPartitionerClass(CustomStripesPartitioner.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/stripes"));

		job1.waitForCompletion(true);
/*Job 3- to calculate the conditional probability for a triplet using pair*/
		Configuration configuration2 = new Configuration();
		configuration2.setLong(FileInputFormat.SPLIT_MAXSIZE, (long) 1048576);
		Job job2 = new Job(configuration2);
		job2.setJarByClass(Driver.class);

		job2.setNumReduceTasks(2);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(MapTripletPairs.class);
		job2.setCombinerClass(CombineTripletPairs.class);
		job2.setReducerClass(ReduceTripletPairs.class);
		job2.setPartitionerClass(CustomStripesPartitioner.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2,
				new Path(args[1] + "/tripletpairs"));
		job2.waitForCompletion(true);
		/*Job 4- to calculate the conditional probability for a triplet using stripes*/
		Configuration configuration3 = new Configuration();
		configuration3.setLong(FileInputFormat.SPLIT_MAXSIZE, (long) 1048576);
		Job job3 = new Job(configuration3);
		job3.setJarByClass(Driver.class);

		job3.setNumReduceTasks(2);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setMapperClass(MapTripletStripes.class);
		job3.setCombinerClass(CombineTripletStripes.class);
		job3.setReducerClass(ReduceTripletStripes.class);
		job3.setPartitionerClass(CustomStripesPartitioner.class);

		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]
				+ "/tripletstripes"));
//Wait for all the jobs to complete
		boolean success = job3.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Driver(), args);
		System.exit(ret);
	}

}
