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
import java.util.List;
import java.util.Vector;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.cache.OLevel2RecordCache.STRATEGY;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
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
	public TransactionConsistencyTest(@Optional(value = "memory:test") String iURL) throws IOException {
		url = iURL;
	}

	@BeforeClass
	public void init() {
		ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
		if ("memory:test".equals(database.getURL()))
			database.create();
	}

	@Test
	public void test1RollbackOnConcurrentException() throws IOException {
		database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
		database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

		database1.begin(TXTYPE.OPTIMISTIC);

		// Create docA.
		ODocument vDocA_db1 = database1.newInstance();
		vDocA_db1.field(NAME, "docA");
		database1.save(vDocA_db1);

		// Create docB.
		ODocument vDocB_db1 = database1.newInstance();
		vDocB_db1.field(NAME, "docB");
		database1.save(vDocB_db1);

		database1.commit();

		// Keep the IDs.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();
		ORID vDocB_Rid = vDocB_db1.getIdentity().copy();

		int vDocA_version = -1;
		int vDocB_version = -1;

		database2.begin(TXTYPE.OPTIMISTIC);
		try {
			// Get docA and update in db2 transaction context
			ODocument vDocA_db2 = database2.load(vDocA_Rid);
			vDocA_db2.field(NAME, "docA_v2");
			database2.save(vDocA_db2);

			// Concurrent update docA via database1 -> will throw OConcurrentModificationException at database2.commit().
			database1.begin(TXTYPE.OPTIMISTIC);
			try {
				vDocA_db1.field(NAME, "docA_v3");
				database1.save(vDocA_db1);
				database1.commit();
			} catch (OConcurrentModificationException e) {
				Assert.fail("Should not failed here...");
			}
			Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");
			// Keep the last versions.
			// Following updates should failed and reverted.
			vDocA_version = vDocA_db1.getVersion();
			vDocB_version = vDocB_db1.getVersion();

			// Update docB in db2 transaction context -> should be rollbacked.
			ODocument vDocB_db2 = database2.load(vDocB_Rid);
			vDocB_db2.field(NAME, "docB_UpdatedInTranscationThatWillBeRollbacked");
			database2.save(vDocB_db2);

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

		ODocument vDocA_db2 = database2.load(vDocA_Rid);
		Assert.assertEquals(vDocA_db2.field(NAME), "docA_v3");
		Assert.assertEquals(vDocA_db2.getVersion(), vDocA_version);

		// docB should be in the first state : "docB"
		ODocument vDocB_db2 = database2.load(vDocB_Rid);
		Assert.assertEquals(vDocB_db2.field(NAME), "docB");
		Assert.assertEquals(vDocB_db2.getVersion(), vDocB_version);

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
		database1.save(vDocA_db1);

		// Keep the IDs.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

		database2.begin(TXTYPE.OPTIMISTIC);
		try {
			// Get docA and update in db2 transaction context
			ODocument vDocA_db2 = database2.load(vDocA_Rid);
			vDocA_db2.field(NAME, "docA_v2");
			database2.save(vDocA_db2);

			database1.begin(TXTYPE.OPTIMISTIC);
			try {
				vDocA_db1.field(NAME, "docA_v3");
				database1.save(vDocA_db1);
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
		database1.save(vDocA_db1);

		// Keep the IDs.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

		database2.begin(TXTYPE.OPTIMISTIC);
		try {
			// Get docA and update in db2 transaction context
			ODocument vDocA_db2 = database2.load(vDocA_Rid);
			vDocA_db2.field(NAME, "docA_v2");
			database2.save(vDocA_db2);

			database1.begin(TXTYPE.OPTIMISTIC);
			try {
				vDocA_db1.field(NAME, "docA_v3");
				database1.save(vDocA_db1);
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
		database1.save(vDocA_db1);
		database1.commit();

		// Keep the ID.
		ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

		// Update docA in db2
		database2.begin(TXTYPE.OPTIMISTIC);
		ODocument vDocA_db2 = database2.load(vDocA_Rid);
		vDocA_db2.field(NAME, "docA_v2");
		database2.save(vDocA_db2);
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

		ODocument kim = new ODocument("Profile").field("name", "Kim").field("surname", "Bauer");
		ODocument teri = new ODocument("Profile").field("name", "Teri").field("surname", "Bauer");
		ODocument jack = new ODocument("Profile").field("name", "Jack").field("surname", "Bauer");

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

	@SuppressWarnings("unchecked")
	@Test
	public void createLinkinTx() {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");

		OClass profile = db.getMetadata().getSchema()
				.createClass("MyProfile", db.addCluster("myprofile", OStorage.CLUSTER_TYPE.PHYSICAL));
		OClass edge = db.getMetadata().getSchema().createClass("MyEdge", db.addCluster("myedge", OStorage.CLUSTER_TYPE.PHYSICAL));
		profile.createProperty("name", OType.STRING).setMin("3").setMax("30").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
		profile.createProperty("surname", OType.STRING).setMin("3").setMax("30");
		profile.createProperty("in", OType.LINKSET, edge);
		profile.createProperty("out", OType.LINKSET, edge);
		edge.createProperty("in", OType.LINK, profile);
		edge.createProperty("out", OType.LINK, profile);

		db.begin();

		ODocument kim = new ODocument("MyProfile").field("name", "Kim").field("surname", "Bauer");
		ODocument teri = new ODocument("MyProfile").field("name", "Teri").field("surname", "Bauer");
		ODocument jack = new ODocument("MyProfile").field("name", "Jack").field("surname", "Bauer");

		ODocument myedge = new ODocument("MyEdge").field("in", kim).field("out", jack);
		myedge.save();
		((HashSet<ODocument>) kim.field("out", new HashSet<ORID>()).field("out")).add(myedge);
		((HashSet<ODocument>) jack.field("in", new HashSet<ORID>()).field("in")).add(myedge);

		jack.save();
		kim.save();
		teri.save();

		db.commit();
		db.close();

		db.open("admin", "admin");
		List<ODocument> result = db.command(new OSQLSynchQuery<ODocument>("select from MyProfile ")).execute();

		Assert.assertTrue(result.size() != 0);

		db.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void loadRecordTest() {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");

		try {
			db.begin();

			ODocument kim = new ODocument("Profile").field("name", "Kim").field("surname", "Bauer");
			ODocument teri = new ODocument("Profile").field("name", "Teri").field("surname", "Bauer");
			ODocument jack = new ODocument("Profile").field("name", "Jack").field("surname", "Bauer");
			ODocument chloe = new ODocument("Profile").field("name", "Chloe").field("surname", "O'Brien");

			((HashSet<ODocument>) jack.field("following", new HashSet<ODocument>()).field("following")).add(kim);
			((HashSet<ODocument>) kim.field("following", new HashSet<ODocument>()).field("following")).add(teri);
			((HashSet<ODocument>) teri.field("following", new HashSet<ODocument>()).field("following")).add(jack);
			((HashSet<ODocument>) teri.field("following")).add(kim);
			((HashSet<ODocument>) chloe.field("following", new HashSet<ODocument>()).field("following")).add(jack);
			((HashSet<ODocument>) chloe.field("following")).add(teri);
			((HashSet<ODocument>) chloe.field("following")).add(kim);

			int profileClusterId = db.getClusterIdByName("Profile");

			jack.save();
			Assert.assertEquals(jack.getIdentity().getClusterId(), profileClusterId);

			kim.save();
			Assert.assertEquals(kim.getIdentity().getClusterId(), profileClusterId);

			teri.save();
			Assert.assertEquals(teri.getIdentity().getClusterId(), profileClusterId);

			chloe.save();
			Assert.assertEquals(chloe.getIdentity().getClusterId(), profileClusterId);

			db.commit();

			Assert.assertEquals(jack.getIdentity().getClusterId(), profileClusterId);
			Assert.assertEquals(kim.getIdentity().getClusterId(), profileClusterId);
			Assert.assertEquals(teri.getIdentity().getClusterId(), profileClusterId);
			Assert.assertEquals(chloe.getIdentity().getClusterId(), profileClusterId);

			db.close();
			db.open("admin", "admin");

			ODocument loadedChloe = db.load(chloe.getIdentity());
			System.out.println(loadedChloe);
		} finally {
			db.close();
		}
	}

	@Test
	public void testTransactionPopulateDelete() {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");

		if (!db.getMetadata().getSchema().existsClass("MyFruit")) {
			OClass fruitClass = db.getMetadata().getSchema().createClass("MyFruit");
			fruitClass.createProperty("name", OType.STRING);
			fruitClass.createProperty("color", OType.STRING);
			fruitClass.createProperty("flavor", OType.STRING);

			db.getMetadata().getSchema().getClass("MyFruit").getProperty("name").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
			db.getMetadata().getSchema().getClass("MyFruit").getProperty("color").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
			db.getMetadata().getSchema().getClass("MyFruit").getProperty("flavor").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
		}
		db.close();

		db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");
		int chunkSize = 500;
		for (int initialValue = 0; initialValue < 10; initialValue++) {
			System.out.println("initialValue = " + initialValue);
			Assert.assertEquals(db.countClusterElements("Fruit"), 0);

			// do insert
			Vector<ODocument> v = new Vector<ODocument>();
			db.begin();
			for (int i = initialValue * chunkSize; i < (initialValue * chunkSize) + chunkSize; i++) {
				ODocument d = new ODocument("MyFruit").field("name", "" + i).field("color", "FOO").field("flavor", "BAR" + i);
				d.save();
				v.addElement(d);

			}
			System.out.println("populate commit");
			db.commit();

			// do delete
			db.begin();
			System.out.println("vector size = " + v.size());
			for (int i = 0; i < v.size(); i++) {
				db.delete((ODocument) v.elementAt(i));
			}
			System.out.println("delete commit");
			db.commit();

			Assert.assertEquals(db.countClusterElements("Fruit"), 0);
		}

		db.close();
	}

	@Test
	public void testConsistencyOnDelete() {
		OGraphDatabase db = new OGraphDatabase(url);
		db.open("admin", "admin");

		if (db.getVertexType("Foo") == null)
			db.createVertexType("Foo");

		try {
			// Step 1
			// Create several foo's
			db.createVertex("Foo").field("address", "test1").save();
			db.createVertex("Foo").field("address", "test2").save();
			db.createVertex("Foo").field("address", "test3").save();

			// just show what is there
			List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select * from Foo"));

			for (ODocument d : result) {
				System.out.println("Vertex: " + d);
			}

			// remove those foos in a transaction
			// Step 2
			db.begin(TXTYPE.OPTIMISTIC);

			// Step 3a
			result = db.query(new OSQLSynchQuery<ODocument>("select * from Foo where address = 'test1'"));
			Assert.assertEquals(1, result.size());
			// Step 4a
			db.removeVertex(result.get(0));

			// Step 3b
			result = db.query(new OSQLSynchQuery<ODocument>("select * from Foo where address = 'test2'"));
			Assert.assertEquals(1, result.size());
			// Step 4b
			db.removeVertex(result.get(0));

			// Step 3c
			result = db.query(new OSQLSynchQuery<ODocument>("select * from Foo where address = 'test3'"));
			Assert.assertEquals(1, result.size());
			// Step 4c
			db.removeVertex(result.get(0));

			// Step 6
			db.commit();

			// just show what is there
			result = db.query(new OSQLSynchQuery<ODocument>("select * from Foo"));

			for (ODocument d : result) {
				System.out.println("Vertex: " + d);
			}

		} finally {
			db.close();
		}
	}

	@Test
	public void deletesWithinTransactionArentWorking() throws IOException {
		OGraphDatabase db = new OGraphDatabase(url);
		db.open("admin", "admin");

		try {
			if (db.getVertexType("Foo") == null)
				db.createVertexType("Foo");
			if (db.getVertexType("Bar") == null)
				db.createVertexType("Bar");
			if (db.getVertexType("Sees") == null)
				db.createEdgeType("Sees");

			// Commenting out the transaction will result in the test succeeding.
			db.begin(TXTYPE.OPTIMISTIC);
			ODocument foo = (ODocument) db.createVertex("Foo").field("prop", "test1").save();

			// Comment out these two lines and the test will succeed. The issue appears to be related to an edge
			// connecting a deleted vertex during a transaction
			ODocument bar = (ODocument) db.createVertex("Bar").field("prop", "test1").save();
			ODocument sees = db.createEdge(foo, bar, "Sees");
			db.commit();

			List<ODocument> foos = db.query(new OSQLSynchQuery("select * from Foo"));
			Assert.assertEquals(foos.size(), 1);

			db.begin(TXTYPE.OPTIMISTIC);
			db.removeVertex(foos.get(0));
			db.commit();

		} finally {
			db.close();
		}
	}

	//
	// @SuppressWarnings("unchecked")
	// @Test
	// public void testTransactionPopulatePartialDelete() {
	// System.out.println("************ testTransactionPopulatePartialDelete *******************");
	// ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
	// db.open("admin", "admin");
	//
	// if (!db.getMetadata().getSchema().existsClass("MyFruit")) {
	// OClass fruitClass = db.getMetadata().getSchema().createClass("MyFruit");
	// fruitClass.createProperty("name", OType.STRING);
	// fruitClass.createProperty("color", OType.STRING);
	//
	// db.getMetadata().getSchema().getClass("MyFruit").getProperty("name").createIndex(OClass.INDEX_TYPE.UNIQUE);
	//
	// db.getMetadata().getSchema().getClass("MyFruit").getProperty("color").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
	// }
	//
	// db.declareIntent(new OIntentMassiveInsert());
	//
	// int passCount = 10;
	// int chunkSize = 1000;
	// for (int pass = 0; pass < passCount; pass++) {
	// System.out.println("pass = " + pass);
	//
	// // do insert
	// Vector<ODocument> recordsToDelete = new Vector<ODocument>();
	// db.begin();
	// for (int i = 0; i < chunkSize; i++) {
	// ODocument d = new ODocument( "MyFruit").field("name", "ABC" + pass + 'K' + i).field("color", "FOO" + pass);
	// d.save();
	// if (i < chunkSize / 2)
	// recordsToDelete.addElement(d);
	// }
	// db.commit();
	//
	// // do delete
	// db.begin();
	// for (int i = 0; i < recordsToDelete.size(); i++) {
	// db.delete((ODocument) recordsToDelete.elementAt(i));
	// }
	// db.commit();
	// }
	//
	// db.declareIntent(null);
	//
	// db.close();
	// System.out.println("************ end testTransactionPopulatePartialDelete *******************");
	// }

	public void TransactionRollbackConstistencyTest() {
		System.out.println("**************************TransactionRollbackConsistencyTest***************************************");
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
		db.open("admin", "admin");
		OClass vertexClass = db.getMetadata().getSchema().createClass("TRVertex");
		OClass edgeClass = db.getMetadata().getSchema().createClass("TREdge");
		vertexClass.createProperty("in", OType.LINKSET, edgeClass);
		vertexClass.createProperty("out", OType.LINKSET, edgeClass);
		edgeClass.createProperty("in", OType.LINK, vertexClass);
		edgeClass.createProperty("out", OType.LINK, vertexClass);

		OClass personClass = db.getMetadata().getSchema().createClass("TRPerson", vertexClass);
		personClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
		personClass.createProperty("surname", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
		personClass.createProperty("version", OType.INTEGER);

		db.getMetadata().getSchema().save();
		db.close();

		final int cnt = 4;

		db.open("admin", "admin");
		db.begin();
		Vector inserted = new Vector();

		for (int i = 0; i < cnt; i++) {
			ODocument person = new ODocument("TRPerson");
			person.field("name", Character.toString((char) ('A' + i)));
			person.field("surname", Character.toString((char) ('A' + (i % 3))));
			person.field("myversion", 0);
			person.field("in", new HashSet<ODocument>());
			person.field("out", new HashSet<ODocument>());

			if (i >= 1) {
				ODocument edge = new ODocument("TREdge");
				edge.field("in", person.getIdentity());
				edge.field("out", inserted.elementAt(i - 1));
				((HashSet<ODocument>) person.field("out")).add(edge);
				((HashSet<ODocument>) ((ODocument) inserted.elementAt(i - 1)).field("in")).add(edge);
				edge.save();
			}
			inserted.add(person);
			person.save();
		}
		db.commit();

		final List<ODocument> result1 = db.command(new OCommandSQL("select from TRPerson")).execute();
		Assert.assertNotNull(result1);
		Assert.assertEquals(result1.size(), 4);
		System.out.println("Before transaction commit");
		for (ODocument d : result1)
			System.out.println(d);

		try {
			db.begin();
			Vector inserted2 = new Vector();

			for (int i = 0; i < cnt; i++) {
				ODocument person = new ODocument("TRPerson");
				person.field("name", Character.toString((char) ('a' + i)));
				person.field("surname", Character.toString((char) ('a' + (i % 3))));
				person.field("myversion", 0);
				person.field("in", new HashSet<ODocument>());
				person.field("out", new HashSet<ODocument>());

				if (i >= 1) {
					ODocument edge = new ODocument("TREdge");
					edge.field("in", person.getIdentity());
					edge.field("out", inserted2.elementAt(i - 1));
					((HashSet<ODocument>) person.field("out")).add(edge);
					((HashSet<ODocument>) ((ODocument) inserted2.elementAt(i - 1)).field("in")).add(edge);
					edge.save();
				}
				inserted2.add(person);
				person.save();
			}

			for (int i = 0; i < cnt; i++) {
				if (i != cnt - 1) {
					((ODocument) inserted.elementAt(i)).field("myversion", 2);
					((ODocument) inserted.elementAt(i)).save();
				}
			}

			((ODocument) inserted.elementAt(cnt - 1)).delete();
			((ODocument) inserted.elementAt(cnt - 2)).setVersion(0);
			((ODocument) inserted.elementAt(cnt - 2)).save();
			db.commit();
			Assert.assertTrue(false);
		} catch (OConcurrentModificationException e) {
			Assert.assertTrue(true);
			db.rollback();
		}

		final List<ODocument> result2 = db.command(new OCommandSQL("select from TRPerson")).execute();
		Assert.assertNotNull(result2);
		System.out.println("After transaction commit failure/rollback");
		for (ODocument d : result2)
			System.out.println(d);
		Assert.assertEquals(result2.size(), 4);

		db.close();
		System.out.println("**************************TransactionRollbackConstistencyTest***************************************");
	}

	@Test
	public void testQueryIsolation() {
		OGraphDatabase db = new OGraphDatabase(url);
		db.open("admin", "admin");

		try {
			db.begin();

			ODocument v1 = db.createVertex();
			v1.field("purpose", "testQueryIsolation");
			v1.save();

			if (!url.startsWith("remote")) {
				List<OIdentifiable> result = db.query(new OSQLSynchQuery<Object>("select from V where purpose = 'testQueryIsolation'"));
				Assert.assertEquals(result.size(), 1);
			}

			db.commit();

			List<OIdentifiable> result = db.query(new OSQLSynchQuery<Object>("select from V where purpose = 'testQueryIsolation'"));
			Assert.assertEquals(result.size(), 1);

		} finally {
			db.close();
		}
	}
}
