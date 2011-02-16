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
import com.orientechnologies.orient.core.intent.OIntentMassiveRead;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@Test(sequential = true)
public class LocalReadGraphVariableDensityTest {
	private static ODatabaseGraphTx	database;

	public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
		OProfiler.getInstance().startRecording();

		database = new ODatabaseGraphTx(System.getProperty("url")).open("admin", "admin");

		database.declareIntent(new OIntentMassiveRead());
		database.begin(TXTYPE.NOTX);

		long time = System.currentTimeMillis();

		int count = readSubNodes(database.getRoot("HighDensityGraph"));

		System.out.println("Read of the entire graph. Total " + count + " nodes in " + ((System.currentTimeMillis() - time) / 1000f)
				+ " sec.");

		System.out.println("Repeating the same operation but with hot cache");

		time = System.currentTimeMillis();

		count = readSubNodes(database.getRoot("HighDensityGraph"));

		System.out.println("Read using the cache of the entire graph. Total " + count + " nodes in "
				+ ((System.currentTimeMillis() - time) / 1000f) + " sec.");

		database.close();
	}

	private static int readSubNodes(final OGraphVertex iNode) {
		int i = 0;
		for (OGraphEdge e : iNode.getOutEdges()) {
			// System.out.print(v.get("id") + " - ");
			++i;

			if (i % 10000 == 0)
				System.out.print(".");
		}

		return i;
	}
}
