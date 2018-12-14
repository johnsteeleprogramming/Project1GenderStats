/**
 * Author:  William John Steele
 * Project: Identify the countries where % of female graduates is less than 30%.
 * Process: The mapper finds the correct categories and outputs the key as 
 * 			(country, category) and the value is (year, percent).  It only 
 * 			outputs percents less than 30%.
 * 			The reducer groups based on country and category.  It then outputs 
 * 			the key as (country, category) and the value as a string of pairs 
 * 			each of which is (year, percent).
 * 			This program separates based on country and category and does not
 * 			group together based how the question was worded.
 * 
 */

package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FemGraPct30Mapper;
import com.revature.reduce.FemGraPct30Reducer;

public class FemGraPct30 {
		
	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			System.out.println("Usage: Gender Stats <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();

		job.setJarByClass(FemGraPct30.class);
		job.setJobName("Female Graduation Rates less than 30%");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FemGraPct30Mapper.class);
		job.setReducerClass(FemGraPct30Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}