package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemEduAvrIncReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String results = "";
		// TODO (year, double) are not printing in numerical order.
		// Come back and organize this if time.
		// Consider create (year, double) class with sortby override.
		for (Text value : values) {
			results = results.concat("  ").concat(value.toString());
		}
		context.write(key, new Text(results));
	}
}