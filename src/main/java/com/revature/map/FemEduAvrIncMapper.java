package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemEduAvrIncMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String removeQuotes(String word){
		word = word.trim();
		if(word.charAt(0) == '\"'){
			word = word.substring(1);
		}
		if(word.charAt(word.length()-1) == '\"'){
			word = word.substring(0, word.length()-1);
		}
		return word.trim();
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		int wordCount = 1;
		String countryName = "";
		boolean isUSA = false;
		boolean isEducation = false;
		boolean isCompleted = false;
		boolean isFemale = false;
		String description = "";
		int year = 1959;
		
		for (String word : line.split(",")) {
			word = removeQuotes(word);
			if(word.length() > 0){
				if(wordCount == 1) {
					wordCount++;
					countryName = word;
				}
				else if(wordCount == 2) {
					wordCount++;
					if(word.contains("USA")){
						isUSA = true;
					}
				}
				else if(wordCount == 3 && isUSA) {
					if(word.toLowerCase().contains("educational")){
						wordCount++;
						isEducation = true;
						description = word;
					}
				}
				else if(wordCount == 4 && isEducation){
					if(!word.toLowerCase().contains("no")){
						wordCount++;
						isCompleted = true;
						description = description.concat(", ").concat(word);
					}
				}
				else if(wordCount == 6 && isCompleted){
					if(word.toLowerCase().contains("female")){
						wordCount++;
						isFemale = true;
						description = description.concat(", ").concat("female");
					}
				}
				else if(wordCount >= 8 && isFemale){
					year++;
					if(isFemale){
						if(Double.parseDouble(word) < 30){						
							String newKey = countryName.concat(" ").concat(description);
							String newValue = ("(" + year + ", ").concat(word).concat(")");
							context.write(new Text(newKey), new Text(newValue));
						}
					}
				}
				else{
					wordCount++;
				}
			}
			else if(wordCount >= 8){
				year++;
			}
		}
	}
}
