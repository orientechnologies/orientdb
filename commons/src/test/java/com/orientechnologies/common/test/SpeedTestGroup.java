package com.orientechnologies.common.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(alwaysRun = true)
public abstract class SpeedTestGroup {
	protected static final int												TIME_WAIT	= 1000;

	protected List<SpeedTestAbstract>									tests			= new ArrayList<SpeedTestAbstract>();
	protected HashMap<String, TreeMap<Long, String>>	results		= new HashMap<String, TreeMap<Long, String>>();

	protected SpeedTestGroup() {
	}

	public void go() {
		for (SpeedTestAbstract test : tests) {
			test.data().go(test);
			Runtime.getRuntime().gc();
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
			}
		}
	}

	protected SpeedTestAbstract addTest(SpeedTestAbstract test) {
		test.data.setTestGroup(this);
		tests.add(test);
		return test;
	}

	public void setResult(String iResultType, String iTestName, long iResult) {
		TreeMap<Long, String> result = results.get(iResultType);
		if (result == null) {
			result = new TreeMap<Long, String>();
			results.put(iResultType, result);
		}
		result.put(iResult, iTestName);
	}

	@AfterClass
	protected void tearDown() throws Exception {
		printResults();
	}

	protected void printResults() {
		System.out.println("FINAL RESULTS (faster is the first one):");

		int i;
		for (Entry<String, TreeMap<Long, String>> result : results.entrySet()) {
			System.out.println("+ " + result.getKey() + ":");

			i = 1;
			long refValue = 0;
			for (Entry<Long, String> entry : result.getValue().entrySet())
				if (i == 1) {
					System.out.println(" " + i++ + ": " + entry.getValue() + " = " + entry.getKey());
					refValue = entry.getKey();
				} else
					System.out.println(" " + i++ + ": " + entry.getValue() + " = " + entry.getKey() + " (+"
							+ (entry.getKey() * 100 / refValue - 100) + "%)");

			System.out.println();
		}
	}
}
