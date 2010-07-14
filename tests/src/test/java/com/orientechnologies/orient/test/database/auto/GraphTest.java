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

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.graph.ODatabaseGraphTx;
import com.orientechnologies.orient.core.record.impl.OArc;
import com.orientechnologies.orient.core.record.impl.ONode;

@Test(sequential = true)
public class GraphTest {
	protected static final int	TOT_RECORDS	= 100;
	protected long							startRecordNumber;
	private ODatabaseGraphTx		database;

	@Parameters(value = "url")
	public GraphTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());
		database = new ODatabaseGraphTx(iURL);
	}

	@Test
	public void populate() {
		database.open("admin", "admin");

		ONode rootNode = database.createNode().set("id", 0);
		ONode newNode;
		ONode currentNode = rootNode;
		database.setRoot("test", currentNode);

		for (int i = 1; i <= TOT_RECORDS; ++i) {

			newNode = database.createNode().set("id", i);
			currentNode.link(newNode).set("createdOn", new Date());

			currentNode = newNode;
		}

		rootNode.save();

		database.close();
	}

	@Test(dependsOnMethods = "populate")
	public void checkPopulation() {
		database.open("admin", "admin");

		int counter = 0;
		ONode currentNode = database.getRoot("test");
		while (!currentNode.getArcs().isEmpty()) {
			Assert.assertEquals(((Number) currentNode.get("id")).intValue(), counter);

			for (OArc arc : currentNode.getArcs()) {
				counter++;
				currentNode = arc.getDestination();
			}
		}

		Assert.assertEquals(counter, TOT_RECORDS);

		database.close();
	}
}
