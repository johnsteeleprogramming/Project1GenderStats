package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemEduAvrIncMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static String[] descriptionCodes = {"SE.TER.CUAT.BA.FE.ZS", 
		"SE.SEC.CUAT.LO.FE.ZS", "SE.SEC.CUAT.PO.FE.ZS", "SE.PRM.CUAT.FE.ZS",
		"SE.TER.CUAT.ST.FE.ZS", "SE.SEC.CUAT.UP.FE.ZS", "SE.TER.CUAT.MS.FE.ZS",
		"SE.TER.HIAT.BA.FE.ZS", "SE.TER.HIAT.DO.FE.ZS", "SE.SEC.HIAT.LO.FE.ZS",
		"SE.TER.HIAT.MS.FE.ZS", "SE.SEC.HIAT.PO.FE.ZS", "SE.PRM.HIAT.FE.ZS",
		"SE.TER.HIAT.ST.FE.ZS", "SE.SEC.HIAT.UP.FE.ZS", "SE.TER.CUAT.DO.FE.ZS"};
	
	private static String[] fileHeaders = {"Country Name","Country Code",
		"Indicator Name","Indicator Code","1960","1961","1962","1963","1964",
		"1965","1966","1967","1968","1969","1970","1971","1972","1973","1974",
		"1975","1976","1977","1978","1979","1980","1981","1982","1983","1984",
		"1985","1986","1987","1988","1989","1990","1991","1992","1993","1994",
		"1995","1996","1997","1998","1999","2000","2001","2002","2003","2004",
		"2005","2006","2007","2008","2009","2010","2011","2012","2013","2014",
		"2015","2016"};

	private static boolean isUSA(String code){
		return code.equals("USA");
	}
	
	private static boolean isDescriptionCode(String code){
		boolean doesContain = false;
		for(String descriptionCode: descriptionCodes){
			if(code.equals(descriptionCode)){
				doesContain = true;
			}
		}
		return doesContain;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
				
		String[] data = value.toString().split("\".\"");
		
		if(isUSA(data[1]) && isDescriptionCode(data[3])){
			String countryName = data[0].substring(1);
			String description = data[2].trim();
			
			for(int index = 44; index < data.length; index++){
				if(!data[index].isEmpty()){
					try{
						// Keep check in here to make sure value is a number.
						double percent = Double.parseDouble(data[index]);
						if(percent >= 0){
							String outputKey = countryName.concat(": ")
														  .concat(description);
							String outputValue = fileHeaders[index]
												  .concat(", ")
												  .concat(data[index]);
							context.write(new Text(outputKey.trim()), 
									new Text(outputValue.trim()));
						}
					} catch (NumberFormatException e){
					}
				}
			}
		}
	}
}
