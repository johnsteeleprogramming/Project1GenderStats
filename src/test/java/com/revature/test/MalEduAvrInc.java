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

import com.revature.map.MalEduAvrIncMapper;
import com.revature.reduce.MalEduAvrIncReducer;

public class MalEduAvrInc {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {

		MalEduAvrIncMapper mapper = new MalEduAvrIncMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		MalEduAvrIncReducer reducer = new MalEduAvrIncReducer();
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
				+ "male (%)\",\"SE.TER.HIAT.BA.MA.ZS\",\"18.32687\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.20876\",\"\",\"\",\"\",\""
				+ "\",\"30.77077\",\"\",\"\",\"\",\"35.82142\",\"36.17118\",\""
				+ "36.80982\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"48.72416\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"36.94468\",\"36.47357\",\"37.20069\",\"\",\"38.06288\",\""
				+ "38.04807\",\"38.31808\",\"39.31583\",\"40.16515\",\"19.9986\",\""
				+ "19.9808\",\"20.31216\",\"\",";

		mapDriver.withInput(new LongWritable(1), new Text(inputTest));

		String outputKeyTest = "United States: Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "male (%)";
	
		String[] outputValueTest = {"2004, 36.94468", "2005, 36.47357", 
				"2006, 37.20069", "2008, 38.06288", "2009, 38.04807",
				"2010, 38.31808", "2011, 39.31583", "2012, 40.16515",
				"2013, 19.9986", "2014, 19.9808", "2015, 20.31216"};

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
				+ "male (%)";
		
		List<Text> values = new ArrayList<Text>();
		
		String[] inputValueTest = {"2004, 36.94468", "2005, 36.47357", 
				"2006, 37.20069", "2008, 38.06288", "2009, 38.04807",
				"2010, 38.31808", "2011, 39.31583", "2012, 40.16515",
				"2013, 19.9986", "2014, 19.9808", "2015, 20.31216"};
		
		for(int index = 0; index < inputValueTest.length; index++){
			values.add(new Text(inputValueTest[index]));
		}

		String expectedOutput = "(2004-2005, -0.47111) (2005-2006, 0.72712) "
				+ "(2006-2008, 0.43109) (2008-2009, -0.01481) "
				+ "(2009-2010, 0.27001) (2010-2011, 0.99775) "
				+ "(2011-2012, 0.84932) (2012-2013, -20.16655) "
				+ "(2013-2014, -0.0178) (2014-2015, 0.33136) "
				+ "(Average Increase Per Year, -1.55124)";
		
		reduceDriver.withInput(new Text(inputKeyTest), values);

		reduceDriver.withOutput(new Text(inputKeyTest), new Text(expectedOutput));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {

		String inputTest = "\"United States\",\"USA\",\"Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "male (%)\",\"SE.TER.HIAT.BA.MA.ZS\",\"18.32687\",\"\",\"\",\""
				+ "\",\"\",\"\",\"\",\"\",\"\",\"\",\"24.20876\",\"\",\"\",\"\",\""
				+ "\",\"30.77077\",\"\",\"\",\"\",\"35.82142\",\"36.17118\",\""
				+ "36.80982\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"\",\"48.72416\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\""
				+ "\",\"36.94468\",\"36.47357\",\"37.20069\",\"\",\"38.06288\",\""
				+ "38.04807\",\"38.31808\",\"39.31583\",\"40.16515\",\"19.9986\",\""
				+ "19.9808\",\"20.31216\",\"\",";
		
		String outputKeyTest = "United States: Educational attainment, "
				+ "completed Bachelor's or equivalent, population 25+ years, "
				+ "male (%)";

		String outputValueTest = "(2004-2005, -0.47111) (2005-2006, 0.72712) "
				+ "(2006-2008, 0.43109) (2008-2009, -0.01481) "
				+ "(2009-2010, 0.27001) (2010-2011, 0.99775) "
				+ "(2011-2012, 0.84932) (2012-2013, -20.16655) "
				+ "(2013-2014, -0.0178) (2014-2015, 0.33136) "
				+ "(Average Increase Per Year, -1.55124)";

		mapReduceDriver.withInput(new LongWritable(1), new Text(inputTest));

		mapReduceDriver.addOutput(new Text(outputKeyTest), new Text(outputValueTest));

		mapReduceDriver.runTest();
	}

}
