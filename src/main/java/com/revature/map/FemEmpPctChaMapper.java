package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemEmpPctChaMapper extends Mapper<LongWritable, Text, Text, Text> {

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
	
	private static String[] descriptionCodes = {"SL.AGR.EMPL.FE.ZS", 
		"SL.IND.EMPL.FE.ZS", "SL.SRV.EMPL.FE.ZS", "SL.EMP.TOTL.SP.FE.ZS",
		"SL.EMP.TOTL.SP.FE.NE.ZS", "SL.EMP.1524.SP.FE.ZS", "SL.EMP.1524.SP.FE.NE.ZS",
		"SL.ISV.IFRM.FE.ZS"};
	
	private static String[] fileHeaders = {"Country Name","Country Code",
		"Indicator Name","Indicator Code","1960","1961","1962","1963","1964",
		"1965","1966","1967","1968","1969","1970","1971","1972","1973","1974",
		"1975","1976","1977","1978","1979","1980","1981","1982","1983","1984",
		"1985","1986","1987","1988","1989","1990","1991","1992","1993","1994",
		"1995","1996","1997","1998","1999","2000","2001","2002","2003","2004",
		"2005","2006","2007","2008","2009","2010","2011","2012","2013","2014",
		"2015","2016"};

	private static boolean isCountryCode(String code){
		boolean doesContain = false;
		if(code.length() != 3){
			return doesContain;
		}
		for(String countryCode: countryCodes){
			if(code.equals(countryCode)){
				doesContain = true;
			}
		}
		return doesContain;
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

	private static int indexOfYear(String year){
		int yearIndex = -1;
		for(int index = 0; index < fileHeaders.length; index++){
			if(fileHeaders[index].equals(year)){
				yearIndex = index;
			}
		}
		return yearIndex;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
				
		String[] data = value.toString().split("\".\"");
		
		if(isCountryCode(data[1]) && isDescriptionCode(data[3])){
			String countryName = data[0].substring(1);
			String description = data[2].trim();
			
			String startYear = "2000";
			int yearIndex = indexOfYear(startYear);
			
			for(int index = yearIndex; index < data.length; index++){
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
