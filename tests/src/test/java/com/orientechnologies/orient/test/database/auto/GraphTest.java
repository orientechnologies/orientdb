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

import com.orientechnologies.orient.core.db.graph.ODatabaseGraphTx;
import com.orientechnologies.orient.core.db.graph.OGraphElement;
import com.orientechnologies.orient.core.db.graph.OGraphVertex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class GraphTest {
	private ODatabaseGraphTx	database;

	@Parameters(value = "url")
	public GraphTest(String iURL) {
		database = new ODatabaseGraphTx(iURL);
	}

	@SuppressWarnings("unused")
	@Test
	public void populate() {
		database.open("admin", "admin");

		OClass vehicleClass = database.getMetadata().getSchema().getClass("GraphVehicle");
		int existants = database.query(new OSQLSynchQuery<OGraphVertex>("select from GraphVehicle")).size();

		OGraphVertex carNode = database.createVertex("GraphCar").set("brand", "Hyundai").set("model", "Coupe").set("year", 2003).save();
		OGraphVertex motoNode = database.createVertex("GraphMotocycle").set("brand", "Yamaha").set("model", "X-City 250")
				.set("year", 2009).save();

		List<OGraphVertex> result = database.query(new OSQLSynchQuery<OGraphVertex>("select from GraphVehicle"));
		Assert.assertEquals(result.size(), 2 + existants);
		for (OGraphVertex v : result) {
			Assert.assertTrue(v.getDocument().getSchemaClass().isSubClassOf(vehicleClass));
		}

		database.close();
	}

	@Test(dependsOnMethods = "populate")
	public void testMultiEdgeWithSameVertex() {
		database.open("admin", "admin");

		OGraphVertex lucaNode = database.createVertex().set("name", "Luca").set("surname", "Garulli").save();
		OGraphVertex carNode = database.createVertex("GraphCar").set("brand", "Hyundai").set("model", "Coupe").set("year", 2003).save();
		OGraphVertex motoNode = database.createVertex("GraphMotocycle").set("brand", "Yamaha").set("model", "X-City 250")
				.set("year", 2009).save();

		lucaNode.link(carNode).setLabel("drives");

		lucaNode.link(carNode).setLabel("owns");
		lucaNode.link(motoNode).setLabel("owns");
		lucaNode.save();

		database.close();

		database.open("admin", "admin");

		List<OGraphElement> result = database.query(new OSQLSynchQuery<OGraphElement>("select from V where name = 'Luca'"));
		Assert.assertEquals(result.size(), 1);

		lucaNode = (OGraphVertex) result.get(0);

		Assert.assertNotNull(lucaNode.getOutEdges("drives"));
		Assert.assertFalse(lucaNode.getOutEdges("drives").isEmpty());
		Assert.assertEquals(lucaNode.getOutEdgeCount(), 3);
		Assert.assertEquals(lucaNode.getInEdgeCount(), 0);

		lucaNode.unlink(carNode);

		Assert.assertEquals(lucaNode.getOutEdgeCount(), 1);
		Assert.assertEquals(lucaNode.getInEdgeCount(), 0);

		database.close();
	}
}
