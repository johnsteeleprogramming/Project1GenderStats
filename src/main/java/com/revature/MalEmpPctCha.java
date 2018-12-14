/**
 * Author:  William John Steele
 * Project:	The percent of change in male employment from the year 2000.
 * Process: The mapper will find the correct categories, only after 2000, and 
 * 			list key as (country, category) and the values as (year, percent).
 * 			The reducer will then group based off keys.  The value will be
 * 			a string of pairs of (year, percent).
 * 			It also includes pair ("Percent of change from 2000-2016", percent).
 * 			Based off the wording of the question, this program kept categories 
 * 			separate to show details and also showed cumulative at end.
 * 
 */

package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MalEmpPctChaMapper;
import com.revature.reduce.MalEmpPctChaReducer;

public class MalEmpPctCha {
	
	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			System.out.println("Usage: Gender Stats <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();

		job.setJarByClass(MalEmpPctCha.class);
		job.setJobName("Percent change in male employment from 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MalEmpPctChaMapper.class);
		job.setReducerClass(MalEmpPctChaReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
