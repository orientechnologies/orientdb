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
import java.util.HashSet;

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

	// static {
	// boolean assertsEnabled = false;
	// assert assertsEnabled = true; // Intentional side effect!!!
	// if (!assertsEnabled)
	// throw new RuntimeException("Asserts must be enabled for this test");
	// }

	@Parameters(value = "url")
	public TransactionConsistencyTest(String iURL) throws IOException {
		url = iURL;
	}

	@Test
	public void test1RollbackOnConcurrentException() throws IOException {
		database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		database1.begin(TXTYPE.OPTIMISTIC);

		// Create docA.
		ODocument vDocA_db1 = database1.newInstance();
		vDocA_db1.field(NAME, "docA");
		vDocA_db1.save();

		// Create docB.
		ODocument vDocB_db1 = database1.newInstance();
		vDocB_db1.field(NAME, "docB");
		vDocB_db1.save();

		database1.commit();

		// Keep the IDs.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();
		ORID vDocB_Rid = vDocB_db1.getIdentity().copy();

		database2.begin(TXTYPE.OPTIMISTIC);
		try {
			// Get docA and update in db2 transaction context
			ODocument vDocA_db2 = database2.load(vDocA_Rid);
			vDocA_db2.field(NAME, "docA_v2");
			vDocA_db2.save();

			// Concurrent update docA via database1 -> will throw OConcurrentModificationException at database2.commit().
			database1.begin(TXTYPE.OPTIMISTIC);
			try {
				vDocA_db1.field(NAME, "docA_v3");
				vDocA_db1.save();
				database1.commit();
			} catch (OConcurrentModificationException e) {
				Assert.fail("Should not failed here...");
			}
			Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");

			// Update docB in db2 transaction context -> should be rollbacked.
			ODocument vDocB_db2 = database2.load(vDocB_Rid);
			vDocB_db2.field(NAME, "docB_UpdatedInTranscationThatWillBeRollbacked");
			vDocB_db2.save();

			// Will throw OConcurrentModificationException
			database2.commit();
			Assert.fail("Should throw OConcurrentModificationException");
		} catch (OConcurrentModificationException e) {
			database2.rollback();
		}

		// Force reload all (to be sure it is not a cache problem)
		database1.close();
		database2.getStorage().close();
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		// docB should be in the first state : "docB"
		ODocument vDocB_db2 = database2.load(vDocB_Rid);
		Assert.assertEquals(vDocB_db2.field(NAME), "docB");

		database1.close();
		database2.close();
	}

	@Test
	public void test4RollbackWithPin() throws IOException {
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
	public void test3RollbackWithCopyCacheStrategy() throws IOException {
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
	public void test5CacheUpdatedMultipleDbs() {
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

	// @Test
	// public void test2RollbackOnRuntimeException() throws IOException {
	// database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
	//
	// if (database1.getURL().startsWith("remote"))
	// // ONLY NOT-REMOTE TESTS
	// return;
	//
	// database1.begin(TXTYPE.OPTIMISTIC);
	//
	// // Create docA.
	// ODocument vDocA = database1.newInstance();
	// vDocA.field(NAME, "docA");
	// vDocA.save();
	//
	// // Create docB.
	// ODocument vDocB = database1.newInstance();
	// vDocB.field(NAME, "docB");
	// vDocB.save();
	//
	// database1.commit();
	//
	// // Keep the IDs.
	// final ORID vDocA_Rid = vDocA.getIdentity().copy();
	// final ORID vDocB_Rid = vDocB.getIdentity().copy();
	//
	// // Inject exception on second writeRecord().
	// TestSimulateError.onDataLocalWriteRecord = new TestSimulateError() {
	// protected int fCountRecordWritten = 0;
	//
	// @Override
	// public boolean checkDataLocalWriteRecord(ODataLocal iODataLocal, long[] iFilePosition, int iClusterSegment,
	// long iClusterPosition, byte[] iContent) {
	// fCountRecordWritten++;
	// if (fCountRecordWritten == 2)
	// throw new RuntimeException("checkDataLocalWriteRecord on #" + iClusterSegment + ":" + iClusterPosition);
	// return true;
	// }
	// };
	//
	// try {
	// database1.begin(TXTYPE.OPTIMISTIC);
	// vDocA.field(NAME, "docA_v2");
	// vDocA.save();
	// vDocB.field(NAME, "docB_v2");
	// vDocB.save();
	// database1.commit();
	// Assert.fail("Should throw Exception");
	// } catch (Exception e) {
	// // Catch Exception and reload records
	// vDocA = database1.load(vDocA_Rid);
	// vDocB = database1.load(vDocB_Rid);
	// }
	//
	// // Check values.
	// Assert.assertEquals(vDocA.field(NAME), "docA");
	// Assert.assertEquals(vDocB.field(NAME), "docB");
	//
	// // Force reload all (to be sure it is not a cache problem)
	// database1.close();
	// database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
	//
	// //
	// ODocument vDocA_db2 = database2.load(vDocA_Rid);
	// Assert.assertEquals(vDocA_db2.field(NAME), "docA");
	//
	// ODocument vDocB_db2 = database2.load(vDocB_Rid);
	// Assert.assertEquals(vDocB_db2.field(NAME), "docB");
	//
	// database2.close();
	//
	// }

	@SuppressWarnings("unchecked")
	@Test
	public void checkVersionsInConnectedDocuments() {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");

		db.begin();

		ODocument kim = new ODocument(db, "Profile").field("name", "Kim").field("surname", "Bauer");
		ODocument teri = new ODocument(db, "Profile").field("name", "Teri").field("surname", "Bauer");
		ODocument jack = new ODocument(db, "Profile").field("name", "Jack").field("surname", "Bauer");

		((HashSet<ODocument>) jack.field("following", new HashSet<ODocument>()).field("following")).add(kim);
		((HashSet<ODocument>) kim.field("following", new HashSet<ODocument>()).field("following")).add(teri);
		((HashSet<ODocument>) teri.field("following", new HashSet<ODocument>()).field("following")).add(jack);

		jack.save();

		db.commit();

		db.close();
		db.open("admin", "admin");

		ODocument loadedJack = db.load(jack.getIdentity());

		int jackLastVersion = loadedJack.getVersion();
		db.begin();
		loadedJack.field("occupation", "agent");
		loadedJack.save();
		db.commit();
		Assert.assertTrue(jackLastVersion != loadedJack.getVersion());

		loadedJack = db.load(jack.getIdentity());
		Assert.assertTrue(jackLastVersion != loadedJack.getVersion());

		db.close();

		db.open("admin", "admin");
		loadedJack = db.load(jack.getIdentity());
		Assert.assertTrue(jackLastVersion != loadedJack.getVersion());
		db.close();
	}
}
