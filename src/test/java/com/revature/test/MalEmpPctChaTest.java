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

import com.revature.map.MalEmpPctChaMapper;
import com.revature.reduce.MalEmpPctChaReducer;

public class MalEmpPctChaTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {

		MalEmpPctChaMapper mapper = new MalEmpPctChaMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		MalEmpPctChaReducer reducer = new MalEmpPctChaReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		
		String inputTest = "\"United States\",\"USA\",\"Employment in agriculture, "
				+ "male (% of male employment)\",\"SL.AGR.EMPL.MA.ZS\",\""
				+ "10.1899995803833\",\"9.85000038146973\",\"9.21000003814697\""
				+ ",\"8.52999973297119\",\"8.11999988555908\",\"7.65000009536743\""
				+ ",\"6.90999984741211\",\"6.65999984741211\",\"6.55999994277954\""
				+ ",\"6.07000017166138\",\"5.84000015258789\",\"5.65999984741211\""
				+ ",\"5.59999990463257\",\"5.44000005722046\",\"5.51000022888184\""
				+ ",\"5.44999980926514\",\"5.15999984741211\",\"4.88000011444092\""
				+ ",\"4.80999994277954\",\"4.65999984741211\",\"4.73999977111816\""
				+ ",\"4.69999980926514\",\"4.8600001335144\",\"4.76000022888184\""
				+ ",\"4.51999998092651\",\"4.23000001907349\",\"4.11999988555908\""
				+ ",\"4.09000015258789\",\"3.94000005722046\",\"3.91000008583069\""
				+ ",\"3.91000008583069\",\"4.03000020980835\",\"4\",\"3.78999996185303\""
				+ ",\"3.83999991416931\",\"3.79999995231628\",\"3.76999998092651\""
				+ ",\"3.66000008583069\",\"3.60999989509583\",\"3.40000009536743\""
				+ ",\"2.53999996185303\",\"2.32999992370605\",\"2.35999989509583\""
				+ ",\"2.30999994277954\",\"2.25999999046326\",\"2.1800000667572\""
				+ ",\"2.15000009536743\",\"2.04999995231628\",\"2.13000011444092\""
				+ ",\"2.1800000667572\",\"2.26999998092651\",\"2.28999996185303\""
				+ ",\"2.15000009536743\",\"2.10999989509583\",\"2.17000007629395\""
				+ ",\"2.30999994277954\",\"2.27999997138977\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(inputTest));

		String outputKeyTest = "United States: Employment in agriculture, "
				+ "male (% of male employment)";
		
		String[] outputValueTest = {"2000, 2.53999996185303", 
				"2001, 2.32999992370605", "2002, 2.35999989509583", 
				"2003, 2.30999994277954", "2004, 2.25999999046326", 
				"2005, 2.1800000667572", "2006, 2.15000009536743", 
				"2007, 2.04999995231628", "2008, 2.13000011444092", 
				"2009, 2.1800000667572", "2010, 2.26999998092651", 
				"2011, 2.28999996185303", "2012, 2.15000009536743", 
				"2013, 2.10999989509583", "2014, 2.17000007629395", 
				"2015, 2.30999994277954"};

		for(int index = 0; index < outputValueTest.length; index++){
			mapDriver.withOutput(new Text(outputKeyTest), 
					new Text(outputValueTest[index]));
		}
		
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		
		String inputKeyTest = "United States: Employment in agriculture, "
				+ "male (% of male employment)";

		List<Text> values = new ArrayList<Text>();

		String[] inputValueTest = {"2000, 2.53999996185303", 
				"2001, 2.32999992370605", "2002, 2.35999989509583", 
				"2003, 2.30999994277954", "2004, 2.25999999046326", 
				"2005, 2.1800000667572", "2006, 2.15000009536743", 
				"2007, 2.04999995231628", "2008, 2.13000011444092", 
				"2009, 2.1800000667572", "2010, 2.26999998092651", 
				"2011, 2.28999996185303", "2012, 2.15000009536743", 
				"2013, 2.10999989509583", "2014, 2.17000007629395", 
				"2015, 2.30999994277954"};
		
		for(int index = 0; index < inputValueTest.length; index++){
			values.add(new Text(inputValueTest[index]));
		}
		
		reduceDriver.withInput(new Text(inputKeyTest), values);

		String expectedOutput = "";
		
		String[] expectedOutputValues = {"(2000-2001, -0.21)", "(2001-2002, 0.03)",
				"(2002-2003, -0.05)", "(2003-2004, -0.05)", "(2004-2005, -0.08)",
				"(2005-2006, -0.03)", "(2006-2007, -0.1)", "(2007-2008, 0.08)",
				"(2008-2009, 0.05)", "(2009-2010, 0.09)", "(2010-2011, 0.02)",
				"(2011-2012, -0.14)", "(2012-2013, -0.04)", "(2013-2014, 0.06)",
				"(2014-2015, 0.14)", "(Percent of change from 2000-2016, -0.23)"};
	
		for(int index = 0; index < expectedOutputValues.length; index++){
			expectedOutput = expectedOutput
					.concat(expectedOutputValues[index])
					.concat(" ");
		}
		
		expectedOutput = expectedOutput.trim();
		
		reduceDriver.withOutput(new Text(inputKeyTest), new Text(expectedOutput));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {

		String inputTest = "\"United States\",\"USA\",\"Employment in agriculture, "
				+ "male (% of male employment)\",\"SL.AGR.EMPL.MA.ZS\",\""
				+ "10.1899995803833\",\"9.85000038146973\",\"9.21000003814697\""
				+ ",\"8.52999973297119\",\"8.11999988555908\",\"7.65000009536743\""
				+ ",\"6.90999984741211\",\"6.65999984741211\",\"6.55999994277954\""
				+ ",\"6.07000017166138\",\"5.84000015258789\",\"5.65999984741211\""
				+ ",\"5.59999990463257\",\"5.44000005722046\",\"5.51000022888184\""
				+ ",\"5.44999980926514\",\"5.15999984741211\",\"4.88000011444092\""
				+ ",\"4.80999994277954\",\"4.65999984741211\",\"4.73999977111816\""
				+ ",\"4.69999980926514\",\"4.8600001335144\",\"4.76000022888184\""
				+ ",\"4.51999998092651\",\"4.23000001907349\",\"4.11999988555908\""
				+ ",\"4.09000015258789\",\"3.94000005722046\",\"3.91000008583069\""
				+ ",\"3.91000008583069\",\"4.03000020980835\",\"4\",\"3.78999996185303\""
				+ ",\"3.83999991416931\",\"3.79999995231628\",\"3.76999998092651\""
				+ ",\"3.66000008583069\",\"3.60999989509583\",\"3.40000009536743\""
				+ ",\"2.53999996185303\",\"2.32999992370605\",\"2.35999989509583\""
				+ ",\"2.30999994277954\",\"2.25999999046326\",\"2.1800000667572\""
				+ ",\"2.15000009536743\",\"2.04999995231628\",\"2.13000011444092\""
				+ ",\"2.1800000667572\",\"2.26999998092651\",\"2.28999996185303\""
				+ ",\"2.15000009536743\",\"2.10999989509583\",\"2.17000007629395\""
				+ ",\"2.30999994277954\",\"2.27999997138977\",";		

		String outputKeyTest = "United States: Employment in agriculture, "
				+ "male (% of male employment)";

		String expectedOutput = "";
		
		String[] expectedOutputValues = {"(2000-2001, -0.21)", "(2001-2002, 0.03)",
				"(2002-2003, -0.05)", "(2003-2004, -0.05)", "(2004-2005, -0.08)",
				"(2005-2006, -0.03)", "(2006-2007, -0.1)", "(2007-2008, 0.08)",
				"(2008-2009, 0.05)", "(2009-2010, 0.09)", "(2010-2011, 0.02)",
				"(2011-2012, -0.14)", "(2012-2013, -0.04)", "(2013-2014, 0.06)",
				"(2014-2015, 0.14)", "(Percent of change from 2000-2016, -0.23)"};
	
		for(int index = 0; index < expectedOutputValues.length; index++){
			expectedOutput = expectedOutput
					.concat(expectedOutputValues[index])
					.concat(" ");
		}
		
		expectedOutput = expectedOutput.trim();
		
		mapReduceDriver.withInput(new LongWritable(1), new Text(inputTest));

		mapReduceDriver.addOutput(new Text(outputKeyTest), new Text(expectedOutput));

		mapReduceDriver.runTest();
	}
	
}
