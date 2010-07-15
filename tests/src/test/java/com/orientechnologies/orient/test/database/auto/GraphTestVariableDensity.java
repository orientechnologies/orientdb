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
import com.orientechnologies.orient.core.db.graph.OGraphVertex;

@Test(sequential = true)
public class GraphTestVariableDensity {
	private static final int	MAX_NODES						= 500000;
	private static final int	MAX_DEEP						= 5;
	private static final int	START_DENSITY				= 184;
	private static final int	DENSITY_FACTOR			= 13;

	private static int				nodeWrittenCounter	= 0;
	private static int				arcWrittenCounter		= 0;
	private static int				nodeReadCounter			= 0;
	private static int				arcReadCounter			= 0;

	private ODatabaseGraphTx	database;

	@Parameters(value = "url")
	public GraphTestVariableDensity(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());
		database = new ODatabaseGraphTx(iURL);
	}

	@Test(dependsOnMethods = "checkPopulation")
	public void populateWithHighDensity() {
		database.open("admin", "admin");

		long time = System.currentTimeMillis();

		OGraphVertex rootNode = database.createVertex().set("id", 0);

		nodeWrittenCounter = 1;
		createSubNodes(rootNode, 0, START_DENSITY);

		long lap = System.currentTimeMillis();

		database.setRoot("HighDensityGraph", rootNode);

		System.out.println("Creation of the graph with deep=" + MAX_DEEP + " and density variable <=30. Total " + nodeWrittenCounter
				+ " nodes and " + arcWrittenCounter + " arcs in " + ((lap - time) / 1000f) + " sec.");
		database.close();
	}

	@Test(dependsOnMethods = "populateWithHighDensity")
	public void checkHighDensityPopulation() {
		database.open("admin", "admin");

		long time = System.currentTimeMillis();

		readSubNodes(database.getRoot("HighDensityGraph"));

		System.out.println("Read of the entire graph with deep=" + MAX_DEEP + " and density variable <=30 Total " + nodeReadCounter
				+ " nodes and " + arcReadCounter + " arcs in " + ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		Assert.assertEquals(nodeReadCounter, nodeWrittenCounter);
	}

	@Test(dependsOnMethods = "checkHighDensityPopulation")
	public void checkHighDensityPopulationHotCache() {

		long time = System.currentTimeMillis();

		nodeReadCounter = arcReadCounter = 0;
		readSubNodes(database.getRoot("HighDensityGraph"));

		System.out.println("Read of the entire graph with deep=" + MAX_DEEP + " and density variable <=30 Total " + nodeReadCounter
				+ " nodes and " + arcReadCounter + " arcs in " + ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		Assert.assertEquals(nodeReadCounter, nodeWrittenCounter);

		database.close();
	}

	private void readSubNodes(final OGraphVertex iNode) {
		Assert.assertEquals(((Number) iNode.get("id")).intValue(), nodeReadCounter);

		nodeReadCounter++;

		for (OGraphVertex node : iNode.browseEdgeDestinations()) {
			arcReadCounter++;
			readSubNodes(node);
		}
	}

	private void createSubNodes(final OGraphVertex iNode, final int iDeepLevel, int iDensity) {
		System.out.println("Creating " + iDensity + " sub nodes...");

		OGraphVertex newNode;

		for (int i = 0; i <= iDensity && nodeWrittenCounter < MAX_NODES; ++i) {
			newNode = database.createVertex().set("id", nodeWrittenCounter++);
			iNode.link(newNode);
			arcWrittenCounter++;

			if (iDeepLevel < MAX_DEEP) {
				if (iDensity * DENSITY_FACTOR / 100 > 0)
					iDensity -= iDensity * DENSITY_FACTOR / 100;
				else
					iDensity -= 1;

				createSubNodes(newNode, iDeepLevel + 1, iDensity);
			}
		}

		iNode.save();
	}

}
