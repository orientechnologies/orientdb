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

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.cache.OLevel2RecordCache.STRATEGY;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@Test
public class TransactionConsistencyTest {
	protected ODatabaseDocumentTx	database1;
	protected ODatabaseDocumentTx	database2;

	protected String							url;

	public static final String		NAME	= "name";

	@Parameters(value = "url")
	public TransactionConsistencyTest(String iURL) {
		url = iURL;
	}

	//
	// @Test
	// public void testRollbackOnConcurrentException() throws IOException {
	// database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
	// database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
	//
	// // database1.begin(TXTYPE.OPTIMISTIC);
	//
	// // Create docA.
	// ODocument vDocA_db1 = database1.newInstance();
	// vDocA_db1.field(NAME, "docA");
	// vDocA_db1.save();
	//
	// // Create docB.
	// ODocument vDocB_db1 = database1.newInstance();
	// vDocB_db1.field(NAME, "docB");
	// vDocB_db1.save();
	//
	// // database1.commit();
	//
	// // Keep the IDs.
	// ORID vDocA_Rid = vDocA_db1.getIdentity().copy();
	// ORID vDocB_Rid = vDocB_db1.getIdentity().copy();
	//
	// database2.begin(TXTYPE.OPTIMISTIC);
	// try {
	// // Get docA and update in db2 transaction context
	// ODocument vDocA_db2 = database2.load(vDocA_Rid);
	// vDocA_db2.field(NAME, "docA_v2");
	// vDocA_db2.save();
	//
	// // Concurrent update docA via database1 -> will throw OConcurrentModificationException at database2.commit().
	// database1.begin(TXTYPE.OPTIMISTIC);
	// try {
	// vDocA_db1.field(NAME, "docA_v3");
	// vDocA_db1.save();
	// database1.commit();
	// } catch (OConcurrentModificationException e) {
	// Assert.fail("Should not failed here...");
	// }
	// Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");
	//
	// // Update docB in db2 transaction context -> should be rollbacked.
	// ODocument vDocB_db2 = database2.load(vDocB_Rid);
	// vDocB_db2.field(NAME, "docB_UpdatedInTranscationThatWillBeRollbacked");
	// vDocB_db2.save();
	//
	// // Will throw OConcurrentModificationException
	// database2.commit();
	// Assert.fail("Should throw OConcurrentModificationException");
	// } catch (OConcurrentModificationException e) {
	// database2.rollback();
	// }
	//
	// // Force reload all (to be sure it is not a cache problem)
	// database1.close();
	// database2.getStorage().close();
	// database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
	//
	// // docB should be in the first state : "docB"
	// ODocument vDocB_db2 = database2.load(vDocB_Rid);
	// Assert.assertEquals(vDocB_db2.field(NAME), "docB");
	//
	// database1.close();
	// database2.close();
	// }

	@Test
	public void testRollbackWithPin() throws IOException {
		database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		// Create docA.
		ODocument vDocA_db1 = database1.newInstance();
		vDocA_db1.field(NAME, "docA");
		vDocA_db1.unpin();
		vDocA_db1.save();

		// Keep the IDs.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

		database2.begin(TXTYPE.OPTIMISTIC);
		try {
			// Get docA and update in db2 transaction context
			ODocument vDocA_db2 = database2.load(vDocA_Rid);
			vDocA_db2.field(NAME, "docA_v2");
			vDocA_db2.save();

			database1.begin(TXTYPE.OPTIMISTIC);
			try {
				vDocA_db1.field(NAME, "docA_v3");
				vDocA_db1.save();
				database1.commit();
			} catch (OConcurrentModificationException e) {
				Assert.fail("Should not failed here...");
			}
			Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");

			// Will throw OConcurrentModificationException
			database2.commit();
			Assert.fail("Should throw OConcurrentModificationException");
		} catch (OConcurrentModificationException e) {
			database2.rollback();
		}

		// Force reload all (to be sure it is not a cache problem)
		database1.close();
		database2.close();
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		// docB should be in the last state : "docA_v3"
		ODocument vDocB_db2 = database2.load(vDocA_Rid);
		Assert.assertEquals(vDocB_db2.field(NAME), "docA_v3");

		database1.close();
		database2.close();
	}

	@Test
	public void testRollbackWithCopyCacheStrategy() throws IOException {
		database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		database1.getLevel2Cache().setStrategy(STRATEGY.COPY_RECORD);

		// Create docA.
		ODocument vDocA_db1 = database1.newInstance();
		vDocA_db1.field(NAME, "docA");
		vDocA_db1.save();

		// Keep the IDs.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

		database2.begin(TXTYPE.OPTIMISTIC);
		try {
			// Get docA and update in db2 transaction context
			ODocument vDocA_db2 = database2.load(vDocA_Rid);
			vDocA_db2.field(NAME, "docA_v2");
			vDocA_db2.save();

			database1.begin(TXTYPE.OPTIMISTIC);
			try {
				vDocA_db1.field(NAME, "docA_v3");
				vDocA_db1.save();
				database1.commit();
			} catch (OConcurrentModificationException e) {
				Assert.fail("Should not failed here...");
			}
			Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");

			// Will throw OConcurrentModificationException
			database2.commit();
			Assert.fail("Should throw OConcurrentModificationException");
		} catch (OConcurrentModificationException e) {
			database2.rollback();
		}

		// Force reload all (to be sure it is not a cache problem)
		database1.close();
		database2.close();
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		// docB should be in the last state : "docA_v3"
		ODocument vDocB_db2 = database2.load(vDocA_Rid);
		Assert.assertEquals(vDocB_db2.field(NAME), "docA_v3");

		database1.close();
		database2.close();
	}

	@Test
	public void testCacheUpdatedMultipleDbs() {
		database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		// Create docA in db1
		database1.begin(TXTYPE.OPTIMISTIC);
		ODocument vDocA_db1 = database1.newInstance();
		vDocA_db1.field(NAME, "docA");
		vDocA_db1.save();
		database1.commit();

		// Keep the ID.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

		// Update docA in db2
		database2.begin(TXTYPE.OPTIMISTIC);
		ODocument vDocA_db2 = database2.load(vDocA_Rid);
		vDocA_db2.field(NAME, "docA_v2");
		vDocA_db2.save();
		database2.commit();

		// Later... read docA with db1.
		database1.begin(TXTYPE.OPTIMISTIC);
		ODocument vDocA_db1_later = database1.load(vDocA_Rid, null, true);
		Assert.assertEquals(vDocA_db1_later.field(NAME), "docA_v2");
		database1.commit();

		database1.close();
		database2.close();
	}
}
