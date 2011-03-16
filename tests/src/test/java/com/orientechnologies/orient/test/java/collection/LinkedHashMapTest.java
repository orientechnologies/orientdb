package com.orientechnologies.orient.test.java.collection;

import java.util.LinkedHashMap;
import java.util.Map;

import org.testng.Assert;

public class LinkedHashMapTest {

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		LinkedHashMap<Integer, Integer> lhm = new LinkedHashMap<Integer, Integer>(100, 0.7f, true) {
			@Override
			protected boolean removeEldestEntry(final Map.Entry<Integer, Integer> iEldest) {
				return size() > 10;
			}
		};

		for (int i = 0; i < 1000000; ++i) {
			lhm.put(i, i);
		}

		Assert.assertEquals(lhm.size(), 10);
	}
}
