package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FemEduAvrIncMapper;
import com.revature.reduce.FemEduAvrIncReducer;

public class FemEduAvrInc {

	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			System.out.println("Usage: Gender Stats <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();

		job.setJarByClass(FemEduAvrInc.class);
		job.setJobName("Average increase in female education since 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FemEduAvrIncMapper.class);
		job.setReducerClass(FemEduAvrIncReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
