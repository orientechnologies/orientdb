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
package com.orientechnologies.orient.test.database.base;

import java.util.List;

import org.testng.annotations.Parameters;

import com.orientechnologies.orient.core.record.ORecord;

public class OrientTest {
	protected static String	url;

	@Parameters(value = "url")
	public OrientTest(String iURL) {
		url = iURL;
	}

	public static void printRecords(List<? extends ORecord<?>> iRecords) {
		int i = 0;
		for (ORecord<?> record : iRecords) {
			printRecord(i, record);
		}
	}

	public static void printRecord(int i, final Object iRecord) {
		if (iRecord != null)
			System.out.println(String.format("%-3d: %s", i, iRecord.toString()));
	}
}
