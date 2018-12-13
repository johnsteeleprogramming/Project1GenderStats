package com.revature.reduce;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemEduAvrIncReducer extends Reducer<Text, Text, Text, Text>{

	public static String treeMapToString(TreeMap<String, Double> tm){
		String mapPrint = "";
		for(Map.Entry<String, Double> entry: tm.entrySet()){
			String key = entry.getKey();
			String value = entry.getValue().toString();
			mapPrint = mapPrint.concat("(")
					.concat(key).concat(", ")
					.concat(value).concat(") ");
		}
		return mapPrint.trim();
	}
	
	public TreeMap<String, Double> averageIncreaseMap(TreeMap<Integer, Double> treeMap){
		TreeMap<String, Double> treeMapAvrInc = new TreeMap<String, Double>();
		if(treeMap.size() > 1){
			boolean firstNode = true;
			int prevYear = 0;
			double prevPercent = 0.0;
			double overallPercent = 0.0;
			for(Map.Entry<Integer, Double> entry: treeMap.entrySet()){
				if(firstNode){
					firstNode = false;
					prevYear = entry.getKey();
					prevPercent = entry.getValue();
				}
				else{
					int currentYear = entry.getKey();
					double currentPercent = entry.getValue();
					
					double increasePerYear 
						= (currentPercent - prevPercent)/(currentYear - prevYear);
					
					String yearDisplay = currentYear + "-" + prevYear;
					treeMapAvrInc.put(yearDisplay, increasePerYear);
					
					prevYear = currentYear;
					prevPercent = currentPercent;
					
					overallPercent += increasePerYear;
				}
			}
			treeMapAvrInc.put("Average Increase Per Year", overallPercent/treeMap.size());
		}
		else{
		}
		return treeMapAvrInc;
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		TreeMap<Integer, Double> treeMapIntake = new TreeMap<Integer, Double>();
		
		for(Text value : values) {
			String yearValue = value.toString();
			int commaLocation = yearValue.indexOf(',');
			int year = Integer.parseInt(yearValue.substring(0, commaLocation));
			double percent = 
					Double.parseDouble(
					yearValue.substring(commaLocation+1, yearValue.length()).trim());
			treeMapIntake.put(year, percent);
		}	
		String outputValue = treeMapToString(averageIncreaseMap(treeMapIntake));		
		context.write(key, new Text(outputValue));
	}
}