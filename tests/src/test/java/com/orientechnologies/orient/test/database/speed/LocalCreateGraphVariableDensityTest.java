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
package com.orientechnologies.orient.test.database.speed;

import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.graph.ODatabaseGraphTx;
import com.orientechnologies.orient.core.db.graph.OGraphVertex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@Test(sequential = true)
public class LocalCreateGraphVariableDensityTest {
	private static final int				MAX_NODES						= 1000000;
	private static final int				MAX_DEEP						= 5;
	private static final int				START_DENSITY				= 184;
	private static final int				DENSITY_FACTOR			= 13;

	private static int							nodeWrittenCounter	= 0;
	private static int							arcWrittenCounter		= 0;
	private static int							maxDeep							= 0;

	private static ODatabaseGraphTx	database;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		OProfiler.getInstance().startRecording();

		database = new ODatabaseGraphTx(System.getProperty("url")).open("admin", "admin");

		database.declareIntent(new OIntentMassiveInsert());
		database.begin(TXTYPE.NOTX);

		long time = System.currentTimeMillis();

		OGraphVertex rootNode = database.createVertex().set("id", 0);

		System.out.println("Creating subnodes: ");

		nodeWrittenCounter = 1;
		createSubNodes(rootNode, 0, START_DENSITY);

		long lap = System.currentTimeMillis();

		database.setRoot("HighDensityGraph", rootNode);

		System.out.println("\nCreation of the graph with depth=" + maxDeep + " and density variable (" + START_DENSITY + "/"
				+ DENSITY_FACTOR + "). Total " + nodeWrittenCounter + " nodes and " + arcWrittenCounter + " arcs in "
				+ ((lap - time) / 1000f) + " sec.");

		database.close();
	}

	private static void createSubNodes(final OGraphVertex iNode, final int iDeepLevel, int iDensity) {
		if (iDeepLevel >= MAX_DEEP || nodeWrittenCounter >= MAX_NODES)
			return;

		// System.out.print(iNode.get("id"));
		// if (iNode.hasInEdges())
		// System.out.print("<" + iNode.getInEdges().get(0).getOut().get("id"));
		//
		// System.out.print("=" + iDensity + " - ");

		if (nodeWrittenCounter % 100000 == 0)
			System.out.print(".");

		if (iDeepLevel > maxDeep)
			maxDeep = iDeepLevel;

		OGraphVertex newNode;

		for (int i = 0; i <= iDensity && nodeWrittenCounter < MAX_NODES; ++i) {
			newNode = database.createVertex().set("id", nodeWrittenCounter++);
			iNode.link(newNode);
			arcWrittenCounter++;
		}

		for (OGraphVertex node : iNode.browseOutEdgesVertexes()) {
			if (iDensity * DENSITY_FACTOR / 100 > 0)
				iDensity -= iDensity * DENSITY_FACTOR / 100;
			else
				iDensity -= 1;

			if (iDensity <= 0)
				iDensity = 1;

			createSubNodes(node, iDeepLevel + 1, iDensity);
		}

		iNode.save();
	}
}
