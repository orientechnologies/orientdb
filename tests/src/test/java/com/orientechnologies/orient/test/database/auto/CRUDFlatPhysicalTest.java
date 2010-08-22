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
package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

@Test(groups = { "crud", "record-csv" }, sequential = true)
public class CRUDFlatPhysicalTest {
	private static final String	CLUSTER_NAME	= "binary";
	protected static final int	TOT_RECORDS		= 100;
	protected long							startRecordNumber;
	private ODatabaseFlat				database;
	private ORecordFlat					record;

	@Parameters(value = "url")
	public CRUDFlatPhysicalTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());

		database = new ODatabaseFlat(iURL);
		record = database.newInstance();
	}

	public void createRaw() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements(CLUSTER_NAME);

		for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
			record.reset();
			record.value(i + "-binary test").save(CLUSTER_NAME);
		}

		database.close();
	}

	@Test(dependsOnMethods = "createRaw")
	public void testCreateRaw() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClusterElements(CLUSTER_NAME) - startRecordNumber, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "testCreateRaw")
	public void readRawWithExpressiveForwardIterator() {
		database.open("admin", "admin");

		String[] fields;

		int i = 0;
		for (ORecordFlat rec : database.browseCluster(CLUSTER_NAME)) {
			fields = rec.value().split("-");

			Assert.assertEquals(Integer.parseInt(fields[0]), i);

			i++;
		}

		Assert.assertEquals(i, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "readRawWithExpressiveForwardIterator")
	public void updateRaw() {
		database.open("admin", "admin");

		int i = 0;
		for (ORecordFlat rec : database.browseCluster(CLUSTER_NAME)) {

			if (i % 2 == 0) {
				rec.value(rec.value() + "+");
				rec.save();
			}

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "updateRaw")
	public void testUpdateRaw() {
		database.open("admin", "admin");

		String[] fields;

		int i = 0;
		for (ORecordFlat rec : database.browseCluster(CLUSTER_NAME)) {
			fields = rec.value().split("-");

			Assert.assertEquals(Integer.parseInt(fields[0]), i);

			if (i % 2 == 0)
				Assert.assertTrue(fields[1].endsWith("+"));
			else
				Assert.assertFalse(fields[1].endsWith("+"));

			i++;
		}

		database.close();
	}
}
