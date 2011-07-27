package com.orientechnologies.orient.test.java.collection;

import java.util.LinkedHashMap;

public class LinkedHashMapPlusObjectTest {

	public static class TestEntry {
		public Object	value;

		public TestEntry(int i) {
			value = i;
		}
	}

	public static void main(String[] args) {
		LinkedHashMap<Integer, TestEntry> lhme = new LinkedHashMap<Integer, TestEntry>();
		LinkedHashMap<Integer, Integer> lhm = new LinkedHashMap<Integer, Integer>();

		for (int i = 0; i < 3000000; ++i) {
			lhm.put(i, i);
			lhme.put(i, new TestEntry(i));
		}

		long start;
		Object v;

		for (int k = 0; k < 10; ++k) {

			start = System.currentTimeMillis();

			for (int i = 0; i < 3000000; ++i) {
				final TestEntry entry = lhme.get(i);
				v = entry.value;
				if (v == null)
					System.out.println("WOW");
			}

			System.out.println("Elapsed entry: " + (System.currentTimeMillis() - start));

			start = System.currentTimeMillis();
			for (int i = 0; i < 3000000; ++i) {
				v = lhm.get(i);
				if (v == null)
					System.out.println("WOW");
			}

			System.out.println("Elapsed simple: " + (System.currentTimeMillis() - start));
		}
	}
}
