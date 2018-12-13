package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FemEduAvrIncMapper;
import com.revature.reduce.FemEduAvrIncReducer;

public class FemEduAvrIncTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {

		FemEduAvrIncMapper mapper = new FemEduAvrIncMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		FemEduAvrIncReducer reducer = new FemEduAvrIncReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {

		String inputTest = "\"United States\",\"USA\",\"Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\""
				+ "22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\""
				+ "36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\""
				+ ",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\""
				+ ",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(inputTest));

		String outputKeyTest = "United States: Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "female (%)";

		String[] outputValueTest = {"2004, 35.37453", "2005, 36.00504", 
				"2006, 37.52263", "2008, 38.44067", "2009, 39.15297", 
				"2010, 39.89922", "2011, 40.53132", "2012, 41.12231", 
				"2013, 20.18248", "2014, 20.38445", "2015, 20.68499"};

		for(int index = 0; index < outputValueTest.length; index++){
			mapDriver.withOutput(new Text(outputKeyTest), 
					new Text(outputValueTest[index]));
		}
		
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		
		String inputKeyTest = "United States: Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "female (%)";
		
		List<Text> values = new ArrayList<Text>();

		String[] inputValueTest = {"2004, 35.37453", "2005, 36.00504", 
				"2006, 37.52263", "2008, 38.44067", "2009, 39.15297", 
				"2010, 39.89922", "2011, 40.53132", "2012, 41.12231", 
				"2013, 20.18248", "2014, 20.38445", "2015, 20.68499"};
		
		for(int index = 0; index < inputValueTest.length; index++){
			values.add(new Text(inputValueTest[index]));
		}

		String expectedOutput = "(2004-2005, 0.63051) (2005-2006, 1.51759) "
				+ "(2006-2008, 0.45902) (2008-2009, 0.7123) (2009-2010, 0.74625) "
				+ "(2010-2011, 0.6321) (2011-2012, 0.59099) (2012-2013, -20.93983) "
				+ "(2013-2014, 0.20197) (2014-2015, 0.30054) "
				+ "(Average Increase Per Year, -1.37714)";
		
		reduceDriver.withInput(new Text(inputKeyTest), values);

		reduceDriver.withOutput(new Text(inputKeyTest), new Text(expectedOutput));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {

		String inputTest = "\"United States\",\"USA\",\"Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\",\"\",\"\",\""
				+ "22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\",\""
				+ "36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\""
				+ ",\"40.53132\",\"41.12231\",\"20.18248\",\"20.38445\",\"20.68499\""
				+ ",\"\",";
		
		String outputKeyTest = "United States: Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "female (%)";

		String outputValueTest = "(2004-2005, 0.63051) (2005-2006, 1.51759) "
				+ "(2006-2008, 0.45902) (2008-2009, 0.7123) (2009-2010, 0.74625) "
				+ "(2010-2011, 0.6321) (2011-2012, 0.59099) (2012-2013, -20.93983) "
				+ "(2013-2014, 0.20197) (2014-2015, 0.30054) "
				+ "(Average Increase Per Year, -1.37714)";

		mapReduceDriver.withInput(new LongWritable(1), new Text(inputTest));

		mapReduceDriver.addOutput(new Text(outputKeyTest), new Text(outputValueTest));

		mapReduceDriver.runTest();
	}
}
