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

import java.util.Collection;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OPropertyIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
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
	public void geoSchema() {
		database.open("admin", "admin");

		final OClass mapPointClass = database.getMetadata().getSchema().createClass("MapPoint");
		mapPointClass.createProperty("x", OType.DOUBLE).createIndex(INDEX_TYPE.NOTUNIQUE);
		mapPointClass.createProperty("y", OType.DOUBLE).createIndex(INDEX_TYPE.NOTUNIQUE);

		final OPropertyIndex xIndex = database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndex();
		Assert.assertNotNull(xIndex);

		final OPropertyIndex yIndex = database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndex();
		Assert.assertNotNull(yIndex);

		database.close();
	}

	@Test(dependsOnMethods = "geoSchema")
	public void checkGeoIndexes() {
		database.open("admin", "admin");

		final OPropertyIndex xIndex = database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndex();
		Assert.assertNotNull(xIndex);

		final OPropertyIndex yIndex = database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndex();
		Assert.assertNotNull(yIndex);

		database.close();
	}

	@Test(dependsOnMethods = "checkGeoIndexes")
	public void queryCreatePoints() {
		database.open("admin", "admin");

		ODocument point = new ODocument(database);

		for (int i = 0; i < 10000; ++i) {
			point.reset();
			point.setClassName("MapPoint");

			point.field("x", (52.20472d + i / 100d));
			point.field("y", (0.14056d + i / 100d));

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

	@Test(dependsOnMethods = "queryDistance")
	public void spatialRange() {
		database.open("admin", "admin");

		final OPropertyIndex xIndex = database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndex();
		Assert.assertNotNull(xIndex);

		final OPropertyIndex yIndex = database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndex();
		Assert.assertNotNull(yIndex);

		final Collection<OIdentifiable> xResult = xIndex.getUnderlying().getValuesBetween(52.20472, 82.20472);
		final Collection<OIdentifiable> yResult = yIndex.getUnderlying().getValuesBetween(0.14056, 30.14056);

		xResult.retainAll(yResult);

		Assert.assertTrue(xResult.size() != 0);

		database.close();
	}
}
