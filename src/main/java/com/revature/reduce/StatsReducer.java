package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
			// Change generics to 'Text, IntWritable, Text, IntWritable'
			// Change parameters to 'Text key, Iterable<IntWritable> values, Context context'
//			int wordCount = 0;
//			for (IntWritable value : values) {
//				wordCount += value.get();
//			}
//			context.write(key, new IntWritable(wordCount));
		// Change generics to 'Text, Text, Text, Text'
		// Change parameters to 'Text key, Iterable<Text> values, Context context'
		int results = 0;

		for (IntWritable value : values) {
			results += value.get();
		}

		context.write(key, new IntWritable(results));
	}
}