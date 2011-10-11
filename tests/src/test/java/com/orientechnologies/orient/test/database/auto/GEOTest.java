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
import java.util.Set;

import com.orientechnologies.orient.core.index.OIndex;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
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
		mapPointClass.createProperty("x", OType.DOUBLE).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
		mapPointClass.createProperty("y", OType.DOUBLE).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

		final Set<OIndex<?>> xIndexes = database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndexes();
		Assert.assertEquals(xIndexes.size(), 1);

		final Set<OIndex<?>> yIndexes = database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndexes();
		Assert.assertEquals(yIndexes.size(), 1);

		database.close();
	}

	@Test(dependsOnMethods = "geoSchema")
	public void checkGeoIndexes() {
		database.open("admin", "admin");

		final Set<OIndex<?>> xIndexes = database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndexes();
		Assert.assertEquals(xIndexes.size(), 1);

		final Set<OIndex<?>> yIndexDefinitions = database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndexes();
		Assert.assertEquals(yIndexDefinitions.size(), 1);

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

	@Test(dependsOnMethods = "queryCreatePoints")
	public void queryDistanceOrdered() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClass("MapPoint"), 10000);

		// MAKE THE FIRST RECORD DIRTY TO TEST IF DISTANCE JUMP IT
		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from MapPoint limit 1")).execute();
		result.get(0).field("x", "--wrong--");
		result.get(0).save();

		result = database.command(
				new OSQLSynchQuery<ODocument>("select distance(x, y,52.20472, 0.14056 ) as distance from MapPoint order by distance desc"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		Double lastDistance = null;
		for (ODocument d : result) {
			if (lastDistance != null && d.field("distance") != null)
				Assert.assertTrue(((Double) d.field("distance")).compareTo(lastDistance) <= 0);
			lastDistance = d.field("distance");
		}

		database.close();
	}

	@Test(dependsOnMethods = "queryCreatePoints")
	public void spatialRange() {
		database.open("admin", "admin");

		final Set<OIndex<?>> xIndexes = database.getMetadata().getSchema().getClass("MapPoint").getProperty("x").getIndexes();
		Assert.assertEquals(xIndexes.size(), 1);

		final Set<OIndex<?>> yIndexes = database.getMetadata().getSchema().getClass("MapPoint").getProperty("y").getIndexes();
		Assert.assertEquals(yIndexes.size(), 1);

		final Collection<OIdentifiable> xResult = xIndexes.iterator().next().getValuesBetween(52.20472, 82.20472);
		final Collection<OIdentifiable> yResult = yIndexes.iterator().next().getValuesBetween(0.14056, 30.14056);

		xResult.retainAll(yResult);

		Assert.assertTrue(xResult.size() != 0);

		database.close();
	}
}
