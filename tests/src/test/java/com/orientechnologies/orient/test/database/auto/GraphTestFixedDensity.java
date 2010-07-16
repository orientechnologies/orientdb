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
import com.orientechnologies.orient.core.db.graph.ODatabaseGraphTx;
import com.orientechnologies.orient.core.db.graph.OGraphEdge;
import com.orientechnologies.orient.core.db.graph.OGraphVertex;

@Test(sequential = true)
public class GraphTestFixedDensity {
	private static final int	MAX_DEEP						= 100;
	private static final int	DENSITY							= 1;

	private ODatabaseGraphTx	database;
	private int								nodeWrittenCounter	= 0;
	private int								nodeReadCounter			= -1;

	@Parameters(value = "url")
	public GraphTestFixedDensity(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());
		database = new ODatabaseGraphTx(iURL);
	}

	@Test
	public void populate() {
		database.open("admin", "admin");

		long time = System.currentTimeMillis();

		OGraphVertex rootNode = database.createVertex().set("id", 0);

		createSubNodes(rootNode, 0);

		database.setRoot("LinearGraph", rootNode);

		System.out.println("Creation of the graph with deep=" + MAX_DEEP + " and fixed density=1"
				+ ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		database.close();
	}

	@Test(dependsOnMethods = "populate")
	public void checkPopulation() {
		database.open("admin", "admin");

		long time = System.currentTimeMillis();

		readSubNodes(database.getRoot("LinearGraph"));

		System.out.println("Read of the entire graph with deep=" + nodeReadCounter + " and fixed density=" + DENSITY + " in "
				+ ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		Assert.assertEquals(nodeReadCounter, nodeWrittenCounter);
	}

	@Test(dependsOnMethods = "populate")
	public void checkPopulationHotCache() {
		long time = System.currentTimeMillis();

		nodeReadCounter = -1;
		readSubNodes(database.getRoot("LinearGraph"));

		System.out.println("Read with hot cache of the entire graph with deep=" + nodeReadCounter + " and fixed density=" + DENSITY
				+ " in " + ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		Assert.assertEquals(nodeReadCounter, nodeWrittenCounter);

		database.close();
	}

	private void readSubNodes(final OGraphVertex iNode) {
		Assert.assertEquals(((Number) iNode.get("id")).intValue(), ++nodeReadCounter);

		for (OGraphEdge edge : iNode.getOutEdges()) {
			readSubNodes(edge.getOut());
		}
	}

	private void createSubNodes(final OGraphVertex iNode, final int iDeepLevel) {
		OGraphVertex newNode;

		for (int i = 0; i < DENSITY; ++i) {
			newNode = database.createVertex().set("id", ++nodeWrittenCounter);
			iNode.link(newNode);

			if (iDeepLevel < MAX_DEEP)
				createSubNodes(newNode, iDeepLevel + 1);
		}

		iNode.save();
	}
}
