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

	@Test
	public void populate() {
		database.open("admin", "admin");

		OClass vehicleClass = database.getMetadata().getSchema().createClass("GraphVehicle")
				.setSuperClass(database.getMetadata().getSchema().getClass(OGraphVertex.class));

		database.getMetadata().getSchema().createClass("GraphCar").setSuperClass(vehicleClass);
		database.getMetadata().getSchema().createClass("GraphMotocycle").setSuperClass(vehicleClass);
		database.getMetadata().getSchema().save();

		OGraphVertex carNode = database.createVertex("GraphCar").set("brand", "Hyundai").set("model", "Coupe").set("year", 2003).save();
		OGraphVertex motoNode = database.createVertex("GraphMotocycle").set("brand", "Yamaha").set("model", "X-City 250")
				.set("year", 2009).save();

		List<OGraphVertex> result = database.query(new OSQLSynchQuery<OGraphVertex>("select from GraphVehicle"));
		Assert.assertEquals(result.size(), 2);
		for (OGraphVertex v : result) {
			Assert.assertTrue(v.getDocument().getSchemaClass().isSubClassOf(vehicleClass));
		}

		database.close();
	}
}
