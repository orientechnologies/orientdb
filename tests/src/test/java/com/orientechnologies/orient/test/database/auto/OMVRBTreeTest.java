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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

@Test
public class OMVRBTreeTest {
	private ODatabaseDocument	database;

	@Test(enabled = false)
	public static void main(String[] args) {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:temp");
		db.create();
		db.close();

		OMVRBTreeTest test = new OMVRBTreeTest("memory:temp");
		test.treeSet();
	}

	@Parameters(value = "url")
	public OMVRBTreeTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void treeSet() {
		database.open("admin", "admin");

		OMVRBTreeRIDSet set = new OMVRBTreeRIDSet("index");
		for (int i = 0; i < 10000; ++i)
			set.add(new ORecordId(10, i));

		Assert.assertEquals(set.size(), 10000);
		ODocument doc = set.getAsDocument();
		doc.save();
		database.close();

		database.open("admin", "admin");
		OMVRBTreeRIDSet set2 = new OMVRBTreeRIDSet(doc.getIdentity());
		Assert.assertEquals(set2.size(), 10000);

		int i = 0;
		for (ORecordId rid : set2)
			Assert.assertEquals(rid.clusterPosition, i++);

		database.close();
	}
}
