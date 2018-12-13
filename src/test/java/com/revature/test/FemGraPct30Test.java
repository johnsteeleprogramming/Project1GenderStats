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

import com.revature.map.FemGraPct30Mapper;
import com.revature.reduce.FemGraPct30Reducer;

public class FemGraPct30Test {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {

		FemGraPct30Mapper mapper = new FemGraPct30Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		FemGraPct30Reducer reducer = new FemGraPct30Reducer();
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
		String[] outputValueTest = {"1960, 14.8", "1970, 18.7", "1975, 22.2",
				"1979, 26.9", "1980, 28.10064", "1981, 28.02803", 
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

		String[] inputValueTest = {"1960, 14.8", "1970, 18.7", "1975, 22.2",
				"1979, 26.9", "1980, 28.10064", "1981, 28.02803", 
				"2013, 20.18248", "2014, 20.38445", "2015, 20.68499"};

		String expectedOutput = "";
		for(int index = 0; index < inputValueTest.length; index++){
			values.add(new Text(inputValueTest[index]));
			expectedOutput = expectedOutput.concat("(").concat(inputValueTest[index])
					.concat(") ");
		}
		expectedOutput = expectedOutput.trim();
		
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
		String outputValueTest = "(1960, 14.8) (1970, 18.7) (1975, 22.2) "
				+ "(1979, 26.9) (1980, 28.10064) (1981, 28.02803) "
				+ "(2013, 20.18248) (2014, 20.38445) (2015, 20.68499)";

		mapReduceDriver.withInput(new LongWritable(1), new Text(inputTest));

		mapReduceDriver.addOutput(new Text(outputKeyTest), new Text(outputValueTest));

		mapReduceDriver.runTest();
	}
}