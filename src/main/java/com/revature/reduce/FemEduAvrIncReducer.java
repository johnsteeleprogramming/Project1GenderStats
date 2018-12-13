package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemEduAvrIncReducer extends Reducer<Text, Text, Text, Text>{

	public static String treeMapToString(TreeMap<String, String> tm){
		String mapPrint = "";
		for(Map.Entry<String, String> entry: tm.entrySet()){
			String key = entry.getKey();
			String value = entry.getValue();
			mapPrint = mapPrint.concat("(")
					.concat(key).concat(", ")
					.concat(value).concat(") ");
		}
		return mapPrint.trim();
	}
	
	public TreeMap<String, String> averageIncreaseMap(TreeMap<Integer, Double> treeMap){
		
		TreeMap<String, String> treeMapAvrInc = new TreeMap<String, String>();
		DecimalFormat decimalFormat = new DecimalFormat("0.#####");
		
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

					String yearDisplay = prevYear + "-" + currentYear;
					double increasePerYear 
						= (currentPercent-prevPercent)/(currentYear-prevYear);

					treeMapAvrInc.put(yearDisplay, decimalFormat.format(increasePerYear));
					
					prevYear = currentYear;
					prevPercent = currentPercent;
					
					overallPercent += increasePerYear;
				}
			}
			treeMapAvrInc.put("Average Increase Per Year", 
					decimalFormat.format(overallPercent/treeMap.size()));
		}
		else{
			String key = treeMap.firstKey().toString();
			double value = treeMap.get(treeMap.firstKey());
			treeMapAvrInc.put("Only one year ".concat(key), decimalFormat.format(value));
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