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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.iterator.ORecordIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = { "crud", "record-vobject" }, sequential = true)
public class CRUDDocumentPhysicalTest {
	protected static final int	TOT_RECORDS	= 100;
	protected long							startRecordNumber;
	private ODatabaseDocumentTx	database;
	private ODocument						record;
	private String							url;
	String											base64;

	@Parameters(value = "url")
	public CRUDDocumentPhysicalTest(final String iURL) {
		url = iURL;
	}

	@Test
	public void testPool() throws IOException {
		final ODatabaseDocumentTx[] dbs = new ODatabaseDocumentTx[ODatabaseDocumentPool.global().getMaxSize()];

		for (int i = 0; i < 10; ++i) {
			for (int db = 0; db < dbs.length; ++db)
				dbs[db] = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
			for (int db = 0; db < dbs.length; ++db)
				dbs[db].close();
		}
	}

	@Test
	public void cleanAll() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
		record = database.newInstance();

		startRecordNumber = database.countClusterElements("Account");

		// DELETE ALL THE RECORDS IN THE CLUSTER
		for (ODocument rec : database.browseCluster("Account"))
			rec.delete();

		Assert.assertEquals(database.countClusterElements("Account"), 0);

		database.close();
	}

	@Test(dependsOnMethods = "cleanAll")
	public void create() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		startRecordNumber = database.countClusterElements("Account");

		record.setClassName("Account");

		byte[] binary = new byte[100];
		for (int b = 0; b < binary.length; ++b)
			binary[b] = (byte) b;

		base64 = OBase64Utils.encodeBytes(binary);

		for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
			record.reset();

			record.field("id", i);
			record.field("name", "Gipsy");
			record.field("location", "Italy");
			record.field("salary", (i + 300));
			record.field("binary", binary);
			record.field("nonSchemaBinary", binary);
			record.field("testLong", 10000000000L); // TEST LONG
			record.field("extra", "This is an extra field not included in the schema");
			record.field("value", (byte) 10);

			record.save("Account");
		}

		database.close();
	}

	@Test(dependsOnMethods = "create")
	public void testCreate() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		Assert.assertEquals(database.countClusterElements("Account") - startRecordNumber, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void readAndBrowseDescendingAndCheckHoleUtilization() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		// BROWSE IN THE OPPOSITE ORDER
		byte[] binary;
		int i = 0;
		ORecordIterator<ODocument> it = database.browseCluster("Account");
		for (it.last(); it.hasPrevious();) {
			ODocument rec = it.previous();

			Assert.assertEquals(((Number) rec.field("id")).intValue(), i);
			Assert.assertEquals(rec.field("name"), "Gipsy");
			Assert.assertEquals(rec.field("location"), "Italy");
			Assert.assertEquals(((Number) rec.field("testLong")).longValue(), 10000000000L);
			Assert.assertEquals(((Number) rec.field("salary")).intValue(), i + 300);
			Assert.assertNotNull(rec.field("extra"));
			Assert.assertEquals(((Byte) rec.field("value", Byte.class)).byteValue(), (byte) 10);

			binary = rec.field("binary", OType.BINARY);

			for (int b = 0; b < binary.length; ++b)
				Assert.assertEquals(binary[b], (byte) b);

			i++;
		}

		Assert.assertEquals(i, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
	public void update() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		int i = 0;
		for (ODocument rec : database.browseCluster("Account")) {

			if (i % 2 == 0)
				rec.field("location", "Spain");

			rec.field("price", i + 100);

			rec.save();

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "update")
	public void testUpdate() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		int i = 0;
		for (ODocument rec : database.browseCluster("Account")) {

			if (i % 2 == 0)
				Assert.assertEquals(rec.field("location"), "Spain");
			else
				Assert.assertEquals(rec.field("location"), "Italy");

			Assert.assertEquals(((Number) rec.field("price")).intValue(), i + 100);

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "testUpdate")
	public void testDoubleChanges() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		ODocument vDoc = database.newInstance();
		vDoc.setClassName("Profile");
		vDoc.field("nick", "JayM1").field("name", "Jay").field("surname", "Miner");
		vDoc.save();

		vDoc = database.load(vDoc.getIdentity());
		vDoc.field("nick", "JayM2");
		vDoc.field("nick", "JayM3");
		vDoc.save();

		Set<OIndex<?>> indexes = database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getIndexes();

		Assert.assertEquals(indexes.size(), 1);

		OIndex indexDefinition = indexes.iterator().next();
		OIdentifiable vOldName = (OIdentifiable) indexDefinition.get("JayM1");
		Assert.assertNull(vOldName);

		OIdentifiable vIntermediateName = (OIdentifiable) indexDefinition.get("JayM2");
		Assert.assertNull(vIntermediateName);

		OIdentifiable vNewName = (OIdentifiable) indexDefinition.get("JayM3");
		Assert.assertNotNull(vNewName);

		database.close();
	}

	@Test(dependsOnMethods = "testDoubleChanges")
	public void testMultiValues() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		ODocument vDoc = database.newInstance();
		vDoc.setClassName("Profile");
		vDoc.field("nick", "Jacky").field("name", "Jack").field("surname", "Tramiel");
		vDoc.save();

		// add a new record with the same name "nameA".
		vDoc = database.newInstance();
		vDoc.setClassName("Profile");
		vDoc.field("nick", "Jack").field("name", "Jack").field("surname", "Bauer");
		vDoc.save();

		Collection<OIndex<?>> indexes = database.getMetadata().getSchema().getClass("Profile").getProperty("name").getIndexes();
		Assert.assertEquals(indexes.size(), 1);

		OIndex<?> indexName = indexes.iterator().next();
		// We must get 2 records for "nameA".
		Collection<OIdentifiable> vName1 = (Collection<OIdentifiable>) indexName.get("Jack");
		Assert.assertEquals(vName1.size(), 2);

		// Remove this last record.
		database.delete(vDoc);

		// We must get 1 record for "nameA".
		vName1 = (Collection<OIdentifiable>) indexName.get("Jack");
		Assert.assertEquals(vName1.size(), 1);

		database.close();
	}

	@Test(dependsOnMethods = "testMultiValues")
	public void testUnderscoreField() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		ODocument vDoc = database.newInstance();
		vDoc.setClassName("Profile");
		vDoc.field("nick", "MostFamousJack").field("name", "Kiefer").field("surname", "Sutherland")
				.field("tag_list", new String[] { "actor", "myth" });
		vDoc.save();

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where name = 'Kiefer' and tag_list.size() > 0 ")).execute();

		Assert.assertEquals(result.size(), 1);
		database.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testDbCacheUpdated() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		ODocument vDoc = database.newInstance();
		vDoc.setClassName("Profile");

		Set<String> tags = new HashSet<String>();
		tags.add("test");
		tags.add("yeah");

		vDoc.field("nick", "Dexter").field("name", "Michael").field("surname", "Hall").field("tag_list", tags);
		vDoc.save();

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where name = 'Michael'"))
				.execute();

		Assert.assertEquals(result.size(), 1);
		ODocument dexter = result.get(0);
		((Collection<String>) dexter.field("tag_list")).add("actor");

		dexter.setDirty();
		dexter.save();

		result = database
				.command(new OSQLSynchQuery<ODocument>("select from Profile where tag_list in 'actor' and tag_list in 'test'")).execute();
		Assert.assertEquals(result.size(), 1);

		database.close();
	}

	@Test(dependsOnMethods = "testUnderscoreField")
	public void testUpdateLazyDirtyPropagation() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		for (ODocument rec : database.browseCluster("Profile")) {
			Assert.assertFalse(rec.isDirty());

			Collection<?> followers = rec.field("followers");
			if (followers != null && followers.size() > 0) {
				followers.remove(followers.iterator().next());
				Assert.assertTrue(rec.isDirty());
				break;
			}
		}

		database.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testNestedEmbeddedMap() {
		ODocument newDoc = new ODocument(database);

		final Map<String, HashMap<?, ?>> map1 = new HashMap<String, HashMap<?, ?>>();
		newDoc.field("map1", map1, OType.EMBEDDEDMAP);

		final Map<String, HashMap<?, ?>> map2 = new HashMap<String, HashMap<?, ?>>();
		map1.put("map2", (HashMap<?, ?>) map2);

		final Map<String, HashMap<?, ?>> map3 = new HashMap<String, HashMap<?, ?>>();
		map2.put("map3", (HashMap<?, ?>) map3);

		final ORecordId rid = (ORecordId) newDoc.save().getIdentity();

		final ODocument loadedDoc = database.load(rid);

		Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

		Assert.assertTrue(loadedDoc.containsField("map1"));
		Assert.assertTrue(loadedDoc.field("map1") instanceof Map<?, ?>);
		final Map<String, ODocument> loadedMap1 = loadedDoc.field("map1");
		Assert.assertEquals(loadedMap1.size(), 1);

		Assert.assertTrue(loadedMap1.containsKey("map2"));
		Assert.assertTrue(loadedMap1.get("map2") instanceof Map<?, ?>);
		final Map<String, ODocument> loadedMap2 = (Map<String, ODocument>) loadedMap1.get("map2");
		Assert.assertEquals(loadedMap2.size(), 1);

		Assert.assertTrue(loadedMap2.containsKey("map3"));
		Assert.assertTrue(loadedMap2.get("map3") instanceof Map<?, ?>);
		final Map<String, ODocument> loadedMap3 = (Map<String, ODocument>) loadedMap2.get("map3");
		Assert.assertEquals(loadedMap3.size(), 0);
	}

	@Test
	public void commandWithPositionalParameters() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = ? and surname = ?");
		List<ODocument> result = database.command(query).execute("Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithPositionalParameters() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = ? and surname = ?");
		List<ODocument> result = database.query(query, "Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void commandWithNamedParameters() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
				"select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<ODocument> result = database.command(query).execute(params);

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void commandWrongParameterNames() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		ODocument doc = database.newInstance();

		try {
			doc.field("a:b", 10);
			Assert.assertFalse(true);
		} catch (IllegalArgumentException e) {
			Assert.assertTrue(true);
		}

		try {
			doc.field("a,b", 10);
			Assert.assertFalse(true);
		} catch (IllegalArgumentException e) {
			Assert.assertTrue(true);
		}

		database.close();
	}

	@Test
	public void queryWithNamedParameters() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
				"select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<ODocument> result = database.query(query, params);

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	// @Test
	// public void testTransientField() {
	// database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
	//
	// ODocument doc = new ODocument(database, "Profile");
	// doc.field("nick", "LucaPhotoTest");
	// doc.field("photo", "testPhoto"); // THIS IS DECLARED TRANSIENT IN SCHEMA (see SchemaTest.java)
	// doc.save();
	//
	// // RELOAD FROM THE CACHE
	// doc.reload(null, false);
	// Assert.assertEquals(doc.field("nick"), "LucaPhotoTest");
	// Assert.assertTrue(doc.containsField("photo"));
	//
	// // RELOAD FROM DISK
	// doc.reload();
	// Assert.assertEquals(doc.field("nick"), "LucaPhotoTest");
	// Assert.assertFalse(doc.containsField("photo")); // THIS IS DECLARED TRANSIENT IN SCHEMA (see SchemaTest.java)
	//
	// database.close();
	// }
	//
	@Test
	public void testDirtyChild() {
		ODocument parent = new ODocument();

		ODocument child1 = new ODocument().addOwner(parent);
		parent.field("child1", child1);

		Assert.assertTrue(child1.hasOwners());

		ODocument child2 = new ODocument().addOwner(child1);
		child1.field("child2", child2);

		Assert.assertTrue(child2.hasOwners());

		// BEFORE FIRST TOSTREAM
		Assert.assertTrue(parent.isDirty());
		parent.toStream();
		// AFTER TOSTREAM
		Assert.assertTrue(parent.isDirty());
		// CHANGE FIELDS VALUE (Automaticaly set dirty this child)
		child1.field("child2", new ODocument());
		Assert.assertTrue(parent.isDirty());

	}

	@Test
	public void testInvalidFetchplanLoad() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
		ODocument doc = database.newInstance();
		doc.field("test", "test");
		doc.save();
		ORID docRid = doc.getIdentity().copy();
		// RELOAD THE DOCUMENT, THIS WILL PUT IT IN L1 CACHE
		doc = database.load(docRid, "*:-1");
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, docRid);
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(1, 0));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(1, 1));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(1, 2));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(2, 0));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(2, 1));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(2, 2));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(3, 0));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(3, 1));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(3, 2));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(4, 0));
		doc = testInvalidFetchPlanInvalidateL1Cache(doc, new ORecordId(4, 1));
		// CLOSE DB AND RE-TEST THE LOAD TO MAKE SURE
		database.close();
		database = null;
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
		doc = testInvalidFetchPlanClearL1Cache(doc, docRid);
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(1, 0));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(1, 1));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(1, 2));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(2, 0));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(2, 1));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(2, 2));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(3, 0));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(3, 1));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(3, 2));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(4, 0));
		doc = testInvalidFetchPlanClearL1Cache(doc, new ORecordId(4, 1));
		doc = database.load(docRid);
		doc.delete();
		database.close();
	}

	private ODocument testInvalidFetchPlanInvalidateL1Cache(ODocument doc, ORID docRid) {
		try {
			// LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L1 CACHE
			doc = database.load(docRid, "invalid");
			Assert.fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
		}
		// INVALIDATE L1 CACHE TO CHECK THE L2 CACHE
		database.getLevel1Cache().invalidate();
		try {
			// LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L2 CACHE
			doc = database.load(docRid, "invalid");
			Assert.fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
		}
		// CLEAR THE L2 CACHE TO CHECK THE RAW READ
		database.getLevel2Cache().clear();
		try {
			// LOAD DOCUMENT NOT IN ANY CACHE
			doc = database.load(docRid, "invalid");
			Assert.fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
		}
		return doc;
	}

	private ODocument testInvalidFetchPlanClearL1Cache(ODocument doc, ORID docRid) {
		try {
			// LOAD DOCUMENT NOT IN ANY CACHE
			doc = database.load(docRid, "invalid");
			Assert.fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
		}
		// LOAD DOCUMENT, THIS WILL PUT IT IN L1 CACHE
		try {
			database.load(docRid);
		} catch (Exception e) {
		}
		try {
			// LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L1 CACHE
			doc = database.load(docRid, "invalid");
			Assert.fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
		}
		// CLEAR L1 CACHE, THIS WILL PUT IT IN L2 CACHE
		database.getLevel1Cache().clear();
		try {
			// LOAD DOCUMENT, CHECK BEFORE GETTING IT FROM L2 CACHE
			doc = database.load(docRid, "invalid");
			Assert.fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
		}
		// CLEAR THE L2 CACHE TO CHECK THE RAW READ
		database.getLevel2Cache().clear();
		try {
			// LOAD DOCUMENT NOT IN ANY CACHE
			doc = database.load(docRid, "invalid");
			Assert.fail("Should throw IllegalArgumentException");
		} catch (Exception e) {
		}
		return doc;
	}

	public void testEncoding() {
		String s = " \r\n\t:;,.|+*/\\=!?[]()'\"";

		ODocument doc = new ODocument(database);
		doc.field("test", s);
		doc.unpin();
		doc.save();

		doc.reload(null, true);
		Assert.assertEquals(doc.field("test"), s);
	}

	@Test
	public void polymorphicQuery() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		final ORecordAbstract<Object> newAccount = new ODocument(database, "Account").field("name", "testInheritanceName").save();

		List<ODocument> superClassResult = database.query(new OSQLSynchQuery<ODocument>("select from Account"));
		List<ODocument> subClassResult = database.query(new OSQLSynchQuery<ODocument>("select from Company"));

		Assert.assertTrue(superClassResult.size() != 0);
		Assert.assertTrue(subClassResult.size() != 0);
		Assert.assertTrue(superClassResult.size() >= subClassResult.size());

		// VERIFY ALL THE SUBCLASS RESULT ARE ALSO CONTAINED IN SUPERCLASS RESULT
		for (ODocument d : subClassResult) {
			Assert.assertTrue(superClassResult.contains(d));
		}

		HashSet<ODocument> browsed = new HashSet<ODocument>();
		for (ODocument d : database.browseClass("Account")) {
			Assert.assertFalse(browsed.contains(d));
			browsed.add(d);
		}

		newAccount.delete();

		database.close();
	}

	@Test
	public void nonPolymorphicQuery() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		final ORecordAbstract<Object> newAccount = new ODocument(database, "Account").field("name", "testInheritanceName").save();

		List<ODocument> superClassResult = database
				.query(new OSQLSynchQuery<ODocument>("select from Account where @class = 'Account'"));
		List<ODocument> subClassResult = database.query(new OSQLSynchQuery<ODocument>("select from Company where @class = 'Company'"));

		Assert.assertTrue(superClassResult.size() != 0);
		Assert.assertTrue(subClassResult.size() != 0);

		// VERIFY ALL THE SUBCLASS RESULT ARE NOT CONTAINED IN SUPERCLASS RESULT
		for (ODocument d : subClassResult) {
			Assert.assertFalse(superClassResult.contains(d));
		}

		HashSet<ODocument> browsed = new HashSet<ODocument>();
		for (ODocument d : database.browseClass("Account")) {
			Assert.assertFalse(browsed.contains(d));
			browsed.add(d);
		}

		newAccount.delete();

		database.close();
	}

}
