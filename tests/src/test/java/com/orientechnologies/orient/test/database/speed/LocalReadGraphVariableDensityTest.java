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
import com.orientechnologies.orient.core.db.graph.OGraphEdge;
import com.orientechnologies.orient.core.db.graph.OGraphVertex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@Test(sequential = true)
public class LocalReadGraphVariableDensityTest {
	private static int							nodeReadCounter	= 0;
	private static int							arcReadCounter	= 0;
	private static int							maxDeep					= 0;

	private static ODatabaseGraphTx	database;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		OProfiler.getInstance().startRecording();

		database = new ODatabaseGraphTx(System.getProperty("url")).open("admin", "admin");

		database.declareIntent(new OIntentMassiveInsert());
		database.begin(TXTYPE.NOTX);

		long time = System.currentTimeMillis();

		readSubNodes(database.getRoot("HighDensityGraph"), 0);

		System.out.println("Read of the entire graph with deep=" + maxDeep + ". Total " + nodeReadCounter + " nodes and "
				+ arcReadCounter + " arcs in " + ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		System.out.println("Repeating the same operation but with hot cache");

		time = System.currentTimeMillis();

		nodeReadCounter = arcReadCounter = maxDeep = 0;
		readSubNodes(database.getRoot("HighDensityGraph"), 0);

		System.out.println("Read using the cache of the entire graph with deep=" + maxDeep + ". Total " + nodeReadCounter
				+ " nodes and " + arcReadCounter + " arcs in " + ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		database.close();
	}

	private static void readSubNodes(final OGraphVertex iNode, final int iCurrentDeepLevel) {
		if (iCurrentDeepLevel > maxDeep)
			maxDeep = iCurrentDeepLevel;

		nodeReadCounter++;

		for (OGraphEdge edge : iNode.getOutEdges()) {
			arcReadCounter++;
			readSubNodes(edge.getOut(), iCurrentDeepLevel + 1);
		}
	}
}
