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

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
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
		int total = 1000;

		OMVRBTreeRIDSet set = new OMVRBTreeRIDSet("index");
		for (int i = 0; i < total; ++i)
			set.add(new ORecordId(10, i));

		Assert.assertEquals(set.size(), total);
		ODocument doc = set.toDocument();
		doc.save();
		database.close();

		database.open("admin", "admin");
		OMVRBTreeRIDSet set2 = new OMVRBTreeRIDSet(doc.getIdentity()).setAutoConvert(false);
		Assert.assertEquals(set2.size(), total);

		// ITERABLE
		int i = 0;
		for (OIdentifiable rid : set2) {
			Assert.assertEquals(rid.getIdentity().getClusterPosition(), i);
			// System.out.println("Adding " + rid);
			i++;
		}
		Assert.assertEquals(i, total);

		final ORID rootRID = doc.field("root", ORecordId.class);

		// ITERATOR REMOVE
		i = 0;
		for (Iterator<OIdentifiable> it = set2.iterator(); it.hasNext();) {
			final OIdentifiable rid = it.next();
			Assert.assertEquals(rid.getIdentity().getClusterPosition(), i);
			// System.out.println("Removing " + rid);
			it.remove();
			i++;
		}
		Assert.assertEquals(i, total);
		Assert.assertEquals(set2.size(), 0);

		Assert.assertNull(database.load(rootRID));

		database.close();
	}
}
