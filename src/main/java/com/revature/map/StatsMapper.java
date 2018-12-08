package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static String[] countryCodes = {"ABW", 
		"AFG", "AGO", "ALB", "AND", "ARB", "ARE", "ARG", "ARM", "ASM", "ATG", 
		"AUS", "AUT", "AZE", "BDI", "BEL", "BEN", "BFA", "BGD", "BGR", "BHR", 
		"BHS", "BIH", "BLR", "BLZ", "BMU", "BOL", "BRA", "BRB", "BRN", "BTN", 
		"BWA", "CAF", "CAN", "CEB", "CHE", "CHI", "CHL", "CHN", "CIV", "CMR", 
		"COD", "COG", "COL", "COM", "CPV", "CRI", "CSS", "CUB", "CUW", "CYM", 
		"CYP", "CZE", "DEU", "DJI", "DMA", "DNK", "DOM", "DZA", "EAP", "EAR", 
		"EAS", "ECA", "ECS", "ECU", "EGY", "EMU", "ERI", "ESP", "EST", "ETH", 
		"EUU", "FCS", "FIN", "FJI", "FRA", "FRO", "FSM", "GAB", "GBR", "GEO", 
		"GHA", "GIB", "GIN", "GMB", "GNB", "GNQ", "GRC", "GRD", "GRL", "GTM", 
		"GUM", "GUY", "HIC", "HKG", "HND", "HPC", "HRV", "HTI", "HUN", "IBD", 
		"IBT", "IDA", "IDB", "IDN", "IDX", "IMN", "IND", "IRL", "IRN", "IRQ", 
		"ISL", "ISR", "ITA", "JAM", "JOR", "JPN", "KAZ", "KEN", "KGZ", "KHM", 
		"KIR", "KNA", "KOR", "KWT", "LAC", "LAO", "LBN", "LBR", "LBY", "LCA", 
		"LCN", "LDC", "LIC", "LIE", "LKA", "LMC", "LMY", "LSO", "LTE", "LTU", 
		"LUX", "LVA", "MAC", "MAF", "MAR", "MCO", "MDA", "MDG", "MDV", "MEA", 
		"MEX", "MHL", "MIC", "MKD", "MLI", "MLT", "MMR", "MNA", "MNE", "MNG", 
		"MNP", "MOZ", "MRT", "MUS", "MWI", "MYS", "NAC", "NAM", "NCL", "NER", 
		"NGA", "NIC", "NLD", "NOR", "NPL", "NRU", "NZL", "OED", "OMN", "OSS", 
		"PAK", "PAN", "PER", "PHL", "PLW", "PNG", "POL", "PRE", "PRI", "PRK", 
		"PRT", "PRY", "PSE", "PSS", "PST", "PYF", "QAT", "ROU", "RUS", "RWA", 
		"SAS", "SAU", "SDN", "SEN", "SGP", "SLB", "SLE", "SLV", "SMR", "SOM", 
		"SRB", "SSA", "SSD", "SSF", "SST", "STP", "SUR", "SVK", "SVN", "SWE", 
		"SWZ", "SXM", "SYC", "SYR", "TCA", "TCD", "TEA", "TEC", "TGO", "THA", 
		"TJK", "TKM", "TLA", "TLS", "TMN", "TON", "TSA", "TSS", "TTO", "TUN", 
		"TUR", "TUV", "TZA", "UGA", "UKR", "UMC", "URY", "USA", "UZB", "VCT", 
		"VEN", "VGB", "VIR", "VNM", "VUT", "WLD", "WSM", "XKX", "YEM", "ZAF", 
		"ZMB", "ZWE"};
	
	public static String[] getCountryCodes(){
		return countryCodes;
	}
	
	private static boolean isCountryCode(String code){
		boolean doesContain = false;
		
		for(String countryCode: getCountryCodes()){
			if(code.equals(countryCode)){
				doesContain = true;
			}
		}
		
		return doesContain;
	}

	private String cleanWord(String word){
		
		while((word.toLowerCase().charAt(0) < 97 || 
				word.toLowerCase().charAt(0) > 122) && 
				word.length() > 2){
			word = word.substring(1);
		}		
		
		while((word.toLowerCase().charAt(word.length()-1) < 97 || 
				word.toLowerCase().charAt(word.length()-1) > 122) && 
				word.length() > 2){
			word = word.substring(0, word.length()-1);
		}
		return word;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		/*
		 * If you are not familiar with the use of regular expressions in
		 * Java code, search the web for "Java Regex Tutorial." 
		 */
		int wordCount = 1;
		String countryCode = "";
		boolean isFemaleEducation = false;
		
		for (String word : line.split(",")) {
			if(wordCount == 1) {
				wordCount++;
			}
			else if(wordCount == 2) {
				wordCount++;
				if(word.length() > 3) {
					if(word.charAt(0) == '\"') {
						word = word.substring(1);
					}
					if(word.charAt(word.length()-1) == '\"') {
						word = word.substring(0, word.length()-1);
					}
					countryCode = word;
				}
			}
			else if(wordCount == 3) {
				wordCount++;
				if(word.toLowerCase().contains("educational") &&
						word.contains("female") &&
						!word.contains("at least")){
					isFemaleEducation = true;
				}
			}
			else {			
				if (word.length() > 0) {
					word = cleanWord(word);
					if(word.length() == 3){
						context.write(new Text(word), new IntWritable(1));
					}
				}
			}
		}
	}
}