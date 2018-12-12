package com.revature.reduce;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemEduAvrIncReducer extends Reducer<Text, Text, Text, Text>{

	public static String treeMapToString(TreeMap<Integer, Double> tm){
		String mapPrint = "";
		for(Map.Entry<Integer, Double> entry: tm.entrySet()){
			String key = entry.getKey().toString();
			String value = entry.getValue().toString();
			mapPrint = mapPrint.concat("(")
					.concat(key).concat(", ")
					.concat(value).concat(") ");
		}
		return mapPrint.trim();
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		TreeMap<Integer, Double> treeMap = new TreeMap<Integer, Double>();
		
		for(Text value : values) {
			String yearValue = value.toString();
			int commaLocation = yearValue.indexOf(',');
			int year = Integer.parseInt(yearValue.substring(0, commaLocation));
			double percent = 
					Double.parseDouble(
					yearValue.substring(commaLocation+1, yearValue.length()).trim());
			treeMap.put(year, percent);
		}
		String outputValue = treeMapToString(treeMap);
		context.write(key, new Text(outputValue));
	}
}