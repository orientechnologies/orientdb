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

import java.io.IOException;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class GraphDatabaseTest {
	private OGraphDatabase	database;
	private String					url;

	@Parameters(value = "url")
	public GraphDatabaseTest(String iURL) {
		database = new OGraphDatabase(iURL);
		url = iURL;
	}

	@Test
	public void testPool() throws IOException {
		final OGraphDatabase[] dbs = new OGraphDatabase[OGraphDatabasePool.global().getMaxSize()];

		for (int i = 0; i < 10; ++i) {
			for (int db = 0; db < dbs.length; ++db)
				dbs[db] = OGraphDatabasePool.global().acquire(url, "admin", "admin");
			for (int db = 0; db < dbs.length; ++db)
				dbs[db].close();
		}
	}

	@Test
	public void populate() {
		database.open("admin", "admin");

		OClass vehicleClass = database.createVertexType("GraphVehicle");

		database.createVertexType("GraphCar", vehicleClass);
		database.createVertexType("GraphMotocycle", "GraphVehicle");

		ODocument carNode = (ODocument) database.createVertex("GraphCar").field("brand", "Hyundai").field("model", "Coupe")
				.field("year", 2003).save();
		ODocument motoNode = (ODocument) database.createVertex("GraphMotocycle").field("brand", "Yamaha").field("model", "X-City 250")
				.field("year", 2009).save();

		database.createEdge(carNode, motoNode).save();

		List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from GraphVehicle"));
		Assert.assertEquals(result.size(), 2);
		for (ODocument v : result) {
			Assert.assertTrue(v.getSchemaClass().isSubClassOf(vehicleClass));
		}

		database.close();
	}

	@Test(dependsOnMethods = "populate")
	public void checkAfterClose() {
		database.open("admin", "admin");
		database.getMetadata().getSchema().reload();

		List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from GraphVehicle"));
		Assert.assertEquals(result.size(), 2);

		ODocument edge1 = null;
		ODocument edge2 = null;

		for (ODocument v : result) {
			Assert.assertTrue(v.getSchemaClass().isSubClassOf("GraphVehicle"));

			if (v.getClassName().equals("GraphCar")) {
				Assert.assertEquals(database.getOutEdges(v).size(), 1);
				edge1 = (ODocument) database.getOutEdges(v).iterator().next();
			} else {
				Assert.assertEquals(database.getInEdges(v).size(), 1);
				edge2 = (ODocument) database.getInEdges(v).iterator().next();
			}
		}

		Assert.assertEquals(edge1, edge2);

		database.close();
	}
}
