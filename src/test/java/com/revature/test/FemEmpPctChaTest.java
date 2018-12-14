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

import com.revature.map.FemEmpPctChaMapper;
import com.revature.reduce.FemEmpPctChaReducer;

public class FemEmpPctChaTest {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

	@Before
	public void setUp() {

		FemEmpPctChaMapper mapper = new FemEmpPctChaMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		FemEmpPctChaReducer reducer = new FemEmpPctChaReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}

	@Test
	public void testMapper() {
		
		String inputTest = "\"United States\",\"USA\",\""
				+ "Employment in agriculture, female (% of female employment)\""
				+ ",\"SL.AGR.EMPL.FE.ZS\",\"4.51000022888184\",\"4.07999992370605\""
				+ ",\"3.88000011444092\",\"3.79999995231628\",\"3.49000000953674\""
				+ ",\"3.28999996185303\",\"2.82999992370605\",\"2.52999997138977\""
				+ ",\"2.36999988555908\",\"2.21000003814697\",\"2.01999998092651\""
				+ ",\"2\",\"2.02999997138977\",\"1.89999997615814\""
				+ ",\"1.75999999046326\",\"1.72000002861023\",\"1.64999997615814\""
				+ ",\"1.63999998569489\",\"1.69000005722046\",\"1.60000002384186\""
				+ ",\"1.55999994277954\",\"1.54999995231628\",\"1.53999996185303\""
				+ ",\"1.53999996185303\",\"1.41999995708466\",\"1.36000001430511\""
				+ ",\"1.3400000333786\",\"1.32000005245209\",\"1.30999994277954\""
				+ ",\"1.29999995231628\",\"1.25999999046326\",\"1.26999998092651\""
				+ ",\"1.24000000953674\",\"1.1599999666214\",\"1.50999999046326\""
				+ ",\"1.52999997138977\",\"1.49000000953674\",\"1.4099999666214\""
				+ ",\"1.36000001430511\",\"1.37000000476837\",\"0.949999988079071\""
				+ ",\"0.930000007152557\",\"0.920000016689301\",\"0.899999976158142\""
				+ ",\"0.839999973773956\",\"0.829999983310699\",\"0.810000002384186\""
				+ ",\"0.720000028610229\",\"0.759999990463257\",\"0.75\""
				+ ",\"0.819999992847443\",\"0.850000023841858\",\"0.839999973773956\""
				+ ",\"0.769999980926514\",\"0.800000011920929\",\"0.860000014305115\""
				+ ",\"0.879999995231628\",";
		
		mapDriver.withInput(new LongWritable(1), new Text(inputTest));

		String outputKeyTest = "United States: Employment in agriculture, "
				+ "female (% of female employment)";
		
		String[] outputValueTest = {"2000, 0.949999988079071", 
				"2001, 0.930000007152557", "2002, 0.920000016689301", 
				"2003, 0.899999976158142", "2004, 0.839999973773956",
				"2005, 0.829999983310699", "2006, 0.810000002384186",
				"2007, 0.720000028610229", "2008, 0.759999990463257",
				"2009, 0.75", "2010, 0.819999992847443",
				"2011, 0.850000023841858", "2012, 0.839999973773956",
				"2013, 0.769999980926514", "2014, 0.800000011920929",
				"2015, 0.860000014305115"};

		for(int index = 0; index < outputValueTest.length; index++){
			mapDriver.withOutput(new Text(outputKeyTest), 
					new Text(outputValueTest[index]));
		}
		
		mapDriver.runTest();
	}

	@Test
	public void testReducer() {
		
		String inputKeyTest = "United States: Employment in agriculture, "
				+ "female (% of female employment)";

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

		String inputTest = "\"United States\",\"USA\",\""
				+ "Employment in agriculture, female (% of female employment)\""
				+ ",\"SL.AGR.EMPL.FE.ZS\",\"4.51000022888184\",\"4.07999992370605\""
				+ ",\"3.88000011444092\",\"3.79999995231628\",\"3.49000000953674\""
				+ ",\"3.28999996185303\",\"2.82999992370605\",\"2.52999997138977\""
				+ ",\"2.36999988555908\",\"2.21000003814697\",\"2.01999998092651\""
				+ ",\"2\",\"2.02999997138977\",\"1.89999997615814\""
				+ ",\"1.75999999046326\",\"1.72000002861023\",\"1.64999997615814\""
				+ ",\"1.63999998569489\",\"1.69000005722046\",\"1.60000002384186\""
				+ ",\"1.55999994277954\",\"1.54999995231628\",\"1.53999996185303\""
				+ ",\"1.53999996185303\",\"1.41999995708466\",\"1.36000001430511\""
				+ ",\"1.3400000333786\",\"1.32000005245209\",\"1.30999994277954\""
				+ ",\"1.29999995231628\",\"1.25999999046326\",\"1.26999998092651\""
				+ ",\"1.24000000953674\",\"1.1599999666214\",\"1.50999999046326\""
				+ ",\"1.52999997138977\",\"1.49000000953674\",\"1.4099999666214\""
				+ ",\"1.36000001430511\",\"1.37000000476837\",\"0.949999988079071\""
				+ ",\"0.930000007152557\",\"0.920000016689301\",\"0.899999976158142\""
				+ ",\"0.839999973773956\",\"0.829999983310699\",\"0.810000002384186\""
				+ ",\"0.720000028610229\",\"0.759999990463257\",\"0.75\""
				+ ",\"0.819999992847443\",\"0.850000023841858\",\"0.839999973773956\""
				+ ",\"0.769999980926514\",\"0.800000011920929\",\"0.860000014305115\""
				+ ",\"0.879999995231628\",";

		String outputKeyTest = "United States: Employment in agriculture, "
				+ "female (% of female employment)";

		String expectedOutput = "(2000-2001, -0.02) (2001-2002, -0.01) "
				+ "(2002-2003, -0.02) (2003-2004, -0.06) (2004-2005, -0.01) "
				+ "(2005-2006, -0.02) (2006-2007, -0.09) (2007-2008, 0.04) "
				+ "(2008-2009, -0.01) (2009-2010, 0.07) (2010-2011, 0.03) "
				+ "(2011-2012, -0.01) (2012-2013, -0.07) (2013-2014, 0.03) "
				+ "(2014-2015, 0.06) (Percent of change from 2000-2016, -0.09)";
				
		mapReduceDriver.withInput(new LongWritable(1), new Text(inputTest));

		mapReduceDriver.addOutput(new Text(outputKeyTest), new Text(expectedOutput));

		mapReduceDriver.runTest();
	}
}