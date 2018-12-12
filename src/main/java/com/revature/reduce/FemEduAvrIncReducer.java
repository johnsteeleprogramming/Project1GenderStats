package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemEduAvrIncReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String results = "";

		for (Text value : values) {
			results = results.concat(value.toString()).concat("  ");
		}
		context.write(key, new Text(results.trim()));
	}
}