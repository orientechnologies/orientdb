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
import com.orientechnologies.orient.core.db.record.ODatabaseColumn;
import com.orientechnologies.orient.core.record.impl.ORecordColumn;

@Test(groups = { "crud", "record-csv" }, sequential = true)
public class CRUDColumnPhysicalTest {
	protected static final String	CLUSTER_NAME	= "csv";
	protected static final int		TOT_RECORDS		= 1000;

	private ODatabaseColumn				database;
	private ORecordColumn					record;

	protected long								startRecordNumber;

	@Parameters(value = "url")
	public CRUDColumnPhysicalTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());
		database = new ODatabaseColumn(iURL);
		record = database.newInstance();
	}

	public void createRaw() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements(CLUSTER_NAME);

		for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
			record.reset();
			record.value(i + ",Gipsy,Cat,European,Italy," + (i + 300) + ".00").save(CLUSTER_NAME);
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
	public void readRaw() {
		database.open("admin", "admin");

		int i = 0;
		for (ORecordColumn rec : database.browseCluster(CLUSTER_NAME)) {
			Assert.assertEquals(Integer.parseInt(rec.next()), i);
			Assert.assertEquals(rec.next(), "Gipsy");
			Assert.assertEquals(rec.next(), "Cat");
			Assert.assertEquals(rec.next(), "European");
			Assert.assertEquals(rec.next(), "Italy");
			Assert.assertEquals(Float.parseFloat(rec.next()), i + 300f);

			i++;
		}

		Assert.assertTrue(i == TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "readRaw")
	public void updateRaw() {
		database.open("admin", "admin");

		int i = 0;
		for (ORecordColumn rec : database.browseCluster(CLUSTER_NAME)) {

			if (i % 2 == 0)
				rec.field(4, "Spain");

			rec.field(5, String.valueOf(Float.parseFloat(rec.field(5)) + 100f));
			rec.save();

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "updateRaw")
	public void testUpdateRaw() {
		database.open("admin", "admin");

		int i = 0;
		for (ORecordColumn rec : database.browseCluster(CLUSTER_NAME)) {

			Assert.assertEquals(Integer.parseInt(rec.next()), i);
			Assert.assertEquals(rec.next(), "Gipsy");
			Assert.assertEquals(rec.next(), "Cat");
			Assert.assertEquals(rec.next(), "European");

			if (i % 2 == 0)
				Assert.assertEquals(rec.next(), "Spain");
			else
				Assert.assertEquals(rec.next(), "Italy");

			Assert.assertEquals(Float.parseFloat(rec.next()), i + 400f);

			i++;
		}

		database.close();
	}
}
