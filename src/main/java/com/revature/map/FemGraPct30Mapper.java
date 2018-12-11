package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemGraPct30Mapper extends Mapper<LongWritable, Text, Text, Text> {

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
		if(code.length() != 3){
			return doesContain;
		}
		for(String countryCode: getCountryCodes()){
			if(code.equals(countryCode)){
				doesContain = true;
			}
		}
		return doesContain;
	}
	
	private String removeQuotes(String word){
		word = word.trim();
		if(word.charAt(0) == '\"'){
			word = word.substring(1);
		}
		if(word.charAt(word.length()-1) == '\"'){
			word = word.substring(0, word.length()-1);
		}
		return word;
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		int wordCount = 1;
		String countryName = "";
//		String countryCode = "";
		boolean isCountry = false;
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
					if(isCountryCode(word)){
						isCountry = true;
//						countryCode = word;
					}
				}
				else if(wordCount == 3 && isCountry) {
					if(word.toLowerCase().contains("educational")){
						wordCount++;
						isEducation = true;
						description = word;
					}
				}
				else if(wordCount == 4 && isEducation){
					if(!word.toLowerCase().contains("no") && 
						!word.toLowerCase().contains("some")){
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
