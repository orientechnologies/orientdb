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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class GEOTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public GEOTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryCreatePoints() {
		database.open("admin", "admin");

		ODocument point = new ODocument(database);

		for (int i = 0; i < 10000; ++i) {
			point.reset();
			point.setClassName("MapPoint");
			
			point.field("x", (double) (52.20472d + i / 10d));
			point.field("y", (double) (0.14056d + i / 10d));
			
			point.save();
		}

		database.close();
	}

	@Test(dependsOnMethods = "queryCreatePoints")
	public void queryDistance() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClass("MapPoint"), 10000);

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from MapPoint where distance(x, y,52.20472, 0.14056 ) <= 30")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {

			Assert.assertEquals(d.getClassName(), "MapPoint");
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}
}
