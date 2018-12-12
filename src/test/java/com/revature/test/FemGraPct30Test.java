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
		
		String inputTest = "\"France\",\"FRA\",\"Educational attainment, "
				+ "completed lower secondary, population 25+ years, female "
				+ "(%)\",\"SE.SEC.HIAT.LO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"34.18065\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"18.72376\",\"\"";
		mapDriver.withInput(new LongWritable(1), new Text(inputTest));

		String outputKeyTest = "France Educational attainment, "
				+ "completed lower secondary, female";
		String outputValueTest = "(2004, 18.72376)";
		mapDriver.withOutput(new Text(outputKeyTest), new Text(outputValueTest));
		
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {

		String key = "France Educational attainment, "
				+ "completed lower secondary, female";

		List<Text> values = new ArrayList<Text>();
		String value1 = "(2004, 18.72376)";
		String value2 = "(2005, 19.54324)";
		values.add(new Text(value1));
		values.add(new Text(value2));
		String expectedOutput = value1.concat("  ").concat(value2);
		
		reduceDriver.withInput(new Text(key), values);

		reduceDriver.withOutput(new Text(key), new Text(expectedOutput));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {

		String inputTest = "\"France\",\"FRA\",\"Educational attainment, "
				+ "completed lower secondary, population 25+ years, female "
				+ "(%)\",\"SE.SEC.HIAT.LO.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"\",\"34.18065\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"18.72376\",\"\"";
		String outputKey = "France Educational attainment, "
				+ "completed lower secondary, female";
		String outputValue = "(2004, 18.72376)";

		mapReduceDriver.withInput(new LongWritable(1), new Text(inputTest));

		mapReduceDriver.addOutput(new Text(outputKey), new Text(outputValue));

		mapReduceDriver.runTest();
	}
}