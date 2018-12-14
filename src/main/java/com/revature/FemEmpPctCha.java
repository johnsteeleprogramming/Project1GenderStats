/**
 * Author:  William John Steele
 * Project: List the % of change in female employment from the year 2000.
 * Process: The mapper will find the correct categories, pulling data only from 
 * 			countries to keep consistent with other questions, so it excludes 
 * 			the country groups.  It lists the keys as (country, category) and 
 * 			values as (year, percent).
 * 			The reducer will then group based off keys.  The key stays as 
 * 			(country, category) and the value will be a string of pairs of 
 * 			(year, percent).
 * 			It also includes a pair ("Percent of change from 2000-2016",
 * 			cumulative percent change)
 * 			Based on the wording of the question, I did not put all categories 
 * 			together in order to show more detail but I included cumulative.
 * 
 */

package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FemEmpPctChaMapper;
import com.revature.reduce.FemEmpPctChaReducer;

public class FemEmpPctCha {

	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			System.out.println("Usage: Gender Stats <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();

		job.setJarByClass(FemEmpPctCha.class);
		job.setJobName("Percent change in female employment from 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FemEmpPctChaMapper.class);
		job.setReducerClass(FemEmpPctChaReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}