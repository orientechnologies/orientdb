package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Collector;
import com.orientechnologies.orient.test.domain.whiz.Mapper;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @since 22.03.12
 */
@Test(groups = {"index"})
public class LinkMapIndexTest {
	private final ODatabaseDocumentTx database;

	@Parameters(value = "url")
	public LinkMapIndexTest(final String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@BeforeClass
	public void setupSchema() {
		database.open("admin", "admin");
		final OClass linkMapIndexTestClass = database.getMetadata().getSchema().createClass("LinkMapIndexTestClass");
		linkMapIndexTestClass.createProperty("linkMap", OType.LINKMAP, OType.LINK);

		linkMapIndexTestClass.createIndex("mapIndexTestKey", OClass.INDEX_TYPE.NOTUNIQUE, "linkMap");
		linkMapIndexTestClass.createIndex("mapIndexTestValue", OClass.INDEX_TYPE.NOTUNIQUE, "linkMap by value");

		database.getMetadata().getSchema().save();
		database.close();
	}

	@AfterClass
	public void destroySchema() {
		database.open("admin", "admin");
		database.getMetadata().getSchema().dropClass("LinkMapIndexTestClass");
		database.getMetadata().getSchema().save();
		database.close();
	}

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() throws Exception {
		database.command(new OCommandSQL("delete from LinkMapIndexTestClass")).execute();
		database.close();
	}

	public void testIndexMap() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();


		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapInTx() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		try {
			database.begin();
			Map<String, ORID> map = new HashMap<String, ORID>();

			map.put("key1", docOne.getIdentity());
			map.put("key2", docTwo.getIdentity());

			final ODocument document = new ODocument("LinkMapIndexTestClass");
			document.field("linkMap", map);
			document.save();
			database.commit();
		} catch (Exception e) {
			database.rollback();
			throw e;
		}

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}


	public void testIndexMapUpdateOne() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> mapOne = new HashMap<String, ORID>();

		mapOne.put("key1", docOne.getIdentity());
		mapOne.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", mapOne);
		document.save();

		final Map<String, ORID> mapTwo = new HashMap<String, ORID>();
		mapTwo.put("key2", docOne.getIdentity());
		mapTwo.put("key3", docThree.getIdentity());

		document.field("linkMap", mapTwo);
		document.save();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key2") && !d.field("key").equals("key3")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateOneTx() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		database.begin();
		try {
			final Map<String, ORID> mapTwo = new HashMap<String, ORID>();

			mapTwo.put("key3", docOne.getIdentity());
			mapTwo.put("key2", docTwo.getIdentity());

			final ODocument document = new ODocument("LinkMapIndexTestClass");
			document.field("linkMap", mapTwo);
			document.save();

			database.commit();
		} catch (Exception e) {
			database.rollback();
			throw e;
		}

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key2") && !d.field("key").equals("key3")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateOneTxRollback() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> mapOne = new HashMap<String, ORID>();

		mapOne.put("key1", docOne.getIdentity());
		mapOne.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", mapOne);
		document.save();

		database.begin();
		final Map<String, ORID> mapTwo = new HashMap<String, ORID>();

		mapTwo.put("key3", docTwo.getIdentity());
		mapTwo.put("key2", docThree.getIdentity());

		document.field("linkMap", mapTwo);
		document.save();
		database.rollback();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key2") && !d.field("key").equals("key1")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docTwo.getIdentity()) && !d.field("key").equals(docOne.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}


	public void testIndexMapAddItem() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " put linkMap = 'key3', "
						+ docThree.getIdentity())).execute();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 3);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2") && !d.field("key").equals("key3")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 3);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity()) &&
							!d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapAddItemTx() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		try {
			database.begin();
			final ODocument loadedDocument = database.load(document.getIdentity());
			loadedDocument.<Map<String, ORID>>field("linkMap").put("key3", docThree.getIdentity());
			loadedDocument.save();
			database.commit();
		} catch (Exception e) {
			database.rollback();
			throw e;
		}

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 3);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2") && !d.field("key").equals("key3")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 3);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity()) &&
							!d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapAddItemTxRollback() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		database.begin();
		final ODocument loadedDocument = database.load(document.getIdentity());
		loadedDocument.<Map<String, ORID>>field("linkMap").put("key3", docThree.getIdentity());
		loadedDocument.save();
		database.rollback();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docTwo.getIdentity()) && !d.field("key").equals(docOne.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}


	public void testIndexMapUpdateItem() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " put linkMap = 'key2'," + docThree.getIdentity())).execute();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateItemInTx() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		try {
			database.begin();
			final ODocument loadedDocument = database.load(document.getIdentity());
			loadedDocument.<Map<String, ORID>>field("linkMap").put("key2", docThree.getIdentity());
			loadedDocument.save();
			database.commit();
		} catch (Exception e) {
			database.rollback();
			throw e;
		}
		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateItemInTxRollback() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		database.begin();
		final ODocument loadedDocument = database.load(document.getIdentity());
		loadedDocument.<Map<String, ORID>>field("linkMap").put("key2", docThree.getIdentity());
		loadedDocument.save();
		database.rollback();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapRemoveItem() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());
		map.put("key3", docThree.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " remove linkMap = 'key2'")).execute();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key3")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapRemoveItemInTx() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());
		map.put("key3", docThree.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		try {
			database.begin();
			final ODocument loadedDocument = database.load(document.getIdentity());
			loadedDocument.<Map<String, ORID>>field("linkMap").remove("key2");
			loadedDocument.save();
			database.commit();
		} catch (Exception e) {
			database.rollback();
			throw e;
		}

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key3")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapRemoveItemInTxRollback() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());
		map.put("key3", docThree.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();


		database.begin();
		final ODocument loadedDocument = database.load(document.getIdentity());
		loadedDocument.<Map<String, ORID>>field("linkMap").remove("key2");
		loadedDocument.save();
		database.rollback();


		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 3);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2") && !d.field("key").equals("key3")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 3);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity()) &&
							!d.field("key").equals(docThree.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}


	public void testIndexMapRemove() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		final ODocument docThree = new ODocument();
		docThree.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();
		document.delete();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 0);

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 0);
	}

	public void testIndexMapRemoveInTx() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		try {
			database.begin();
			document.delete();
			database.commit();
		} catch (Exception e) {
			database.rollback();
			throw e;
		}

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 0);

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 0);
	}

	public void testIndexMapRemoveInTxRollback() throws Exception {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		database.begin();
		document.delete();
		database.rollback();

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 2);
		for (ODocument d : resultByKey) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("key1") && !d.field("key").equals("key2")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
						.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 2);
		for (ODocument d : resultByValue) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapSQL() {
		final ODocument docOne = new ODocument();
		docOne.save();

		final ODocument docTwo = new ODocument();
		docTwo.save();

		Map<String, ORID> map = new HashMap<String, ORID>();

		map.put("key1", docOne.getIdentity());
		map.put("key2", docTwo.getIdentity());

		final ODocument document = new ODocument("LinkMapIndexTestClass");
		document.field("linkMap", map);
		document.save();

		final List<ODocument> resultByKey = database.query(
						new OSQLSynchQuery<ODocument>("select * from LinkMapIndexTestClass where linkMap containskey ?"), "key1");
		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 1);

		Assert.assertEquals(map, document.field("linkMap"));

		final List<ODocument> resultByValue = database.query(new OSQLSynchQuery<ODocument>(
						"select * from LinkMapIndexTestClass where linkMap  containsvalue ?"), docOne.getIdentity());
		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 1);

		Assert.assertEquals(map, document.field("linkMap"));
	}
}