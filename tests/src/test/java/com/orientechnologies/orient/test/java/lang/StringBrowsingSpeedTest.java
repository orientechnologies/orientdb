/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.java.lang;

public class StringBrowsingSpeedTest {
	private final static String	S		= "OrientDB is an Open Source NoSQL DBMS with both the features of Document and Graph DBMSs. It's written in Java and it's amazing fast: can store up to 150,000 records per second on common hardware. Even if it's Document based database the relationships are managed as in Graph Databases with direct connections among records. You can traverse entire or part of trees and graphs of records in few milliseconds. Supports schema-less, schema-full and schema-mixed modes. Has a strong security profiling system based on user and roles and support the SQL between";
	private final static int		MAX	= 1000000000;

	public static final void main(String[] args) {
		long timer = System.currentTimeMillis();
		for (int i = 0; i < MAX; ++i) {
		}

		final long fixed = System.currentTimeMillis() - timer;

		final int len = S.length();

		{
			timer = System.currentTimeMillis();

			final char[] chars = S.toCharArray();
			for (int i = 0; i < MAX; ++i)
				for (int k = 0; k < len; ++k) {
					final char c = chars[k];
				}
			System.out.println("String chars[]: " + (System.currentTimeMillis() - timer - fixed));
		}

		{
			timer = System.currentTimeMillis();

			for (int i = 0; i < MAX; ++i)
				for (int k = 0; k < len; ++k) {
					final char c = S.charAt(k);
				}

			System.out.println("String charAt(): " + (System.currentTimeMillis() - timer - fixed));
		}

	}
}
