package com.orientechnologies.orient.test.database.auto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Collector;
import com.orientechnologies.orient.test.domain.whiz.Mapper;

/**
 * @author LomakiA <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 21.12.11
 */
@Test(groups = { "index" })
public class MapIndexTest {
	private final ODatabaseObjectTx	database;

	@Parameters(value = "url")
	public MapIndexTest(final String iURL) {
		database = new ODatabaseObjectTx(iURL);
	}

	@BeforeClass
	public void setupSchema() {
		database.open("admin", "admin");
		final OClass collector = database.getMetadata().getSchema().createClass("Mapper");
		collector.createProperty("id", OType.STRING);
		collector.createProperty("intMap", OType.EMBEDDEDMAP, OType.INTEGER);

		collector.createIndex("mapIndexTestKey", OClass.INDEX_TYPE.NOTUNIQUE, "intMap");
		collector.createIndex("mapIndexTestValue", OClass.INDEX_TYPE.NOTUNIQUE, "intMap by value");

    final OClass movie = database.getMetadata().getSchema().createClass("MapIndexTestMovie");
    movie.createProperty("title", OType.STRING);
    movie.createProperty("thumbs", OType.EMBEDDEDMAP, OType.INTEGER);

    movie.createIndex("indexForMap", OClass.INDEX_TYPE.NOTUNIQUE, "thumbs by key");

		database.getMetadata().getSchema().save();
		database.close();
	}

	@AfterClass
	public void destroySchema() {
		database.open("admin", "admin");
		database.getMetadata().getSchema().dropClass("Mapper");
		database.getMetadata().getSchema().dropClass("MapIndexTestMovie");
		database.close();
	}

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() throws Exception {
		database.command(new OCommandSQL("delete from Mapper")).execute();
		database.command(new OCommandSQL("delete from MapIndexTestMovie")).execute();
		database.close();
	}

	public void testIndexMap() {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

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

			if (!d.field("key").equals(10) && !d.field("key").equals(20)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapInTx() throws Exception {
		try {
			database.begin();
			final Mapper mapper = new Mapper();
			Map<String, Integer> map = new HashMap<String, Integer>();

			map.put("key1", 10);
			map.put("key2", 20);

			mapper.setIntMap(map);
			database.save(mapper);
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

			if (!d.field("key").equals(10) && !d.field("key").equals(20)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	
	public void testIndexMapUpdateOne() {
		final Mapper mapper = new Mapper();
		Map<String, Integer> mapOne = new HashMap<String, Integer>();

		mapOne.put("key1", 10);
		mapOne.put("key2", 20);

		mapper.setIntMap(mapOne);
		database.save(mapper);

		final Map<String, Integer> mapTwo = new HashMap<String, Integer>();

		mapTwo.put("key3", 30);
		mapTwo.put("key2", 20);

		mapper.setIntMap(mapTwo);
		database.save(mapper);

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

			if (!d.field("key").equals(30) && !d.field("key").equals(20)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateOneTx() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> mapOne = new HashMap<String, Integer>();

		mapOne.put("key1", 10);
		mapOne.put("key2", 20);

		mapper.setIntMap(mapOne);
		database.save(mapper);

		database.begin();
		try {
			final Map<String, Integer> mapTwo = new HashMap<String, Integer>();

			mapTwo.put("key3", 30);
			mapTwo.put("key2", 20);

			mapper.setIntMap(mapTwo);
			database.save(mapper);
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

			if (!d.field("key").equals(30) && !d.field("key").equals(20)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateOneTxRollback() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> mapOne = new HashMap<String, Integer>();

		mapOne.put("key1", 10);
		mapOne.put("key2", 20);

		mapper.setIntMap(mapOne);
		database.save(mapper);

		database.begin();
		final Map<String, Integer> mapTwo = new HashMap<String, Integer>();

		mapTwo.put("key3", 30);
		mapTwo.put("key2", 20);

		mapper.setIntMap(mapTwo);
		database.save(mapper);
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

			if (!d.field("key").equals(10) && !d.field("key").equals(20)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}


	public void testIndexMapAddItem() {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		database.command(new OCommandSQL("UPDATE " + mapper.getId() + " put intMap = 'key3', 30")).execute();

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

			if (!d.field("key").equals(30) && !d.field("key").equals(20) && !d.field("key").equals(10)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapAddItemTx() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		try {
			database.begin();
			final Mapper loadedMapper = (Mapper)database.load(new ORecordId(mapper.getId()));
			loadedMapper.getIntMap().put("key3", 30);
			database.save(loadedMapper);
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

			if (!d.field("key").equals(30) && !d.field("key").equals(20) && !d.field("key").equals(10)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapAddItemTxRollback() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		database.begin();
		final Mapper loadedMapper = (Mapper) database.load(new ORecordId(mapper.getId()));
		loadedMapper.getIntMap().put("key3", 30);
		database.save(loadedMapper);
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

			if (!d.field("key").equals(20) && !d.field("key").equals(10)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}


	public void testIndexMapUpdateItem() {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		database.command(new OCommandSQL("UPDATE " + mapper.getId() + " put intMap = 'key2', 40")).execute();

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

			if (!d.field("key").equals(10) && !d.field("key").equals(40)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateItemInTx() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		try{
			database.begin();
			final Mapper loadedMapper = (Mapper)database.load(new ORecordId(mapper.getId()));
			loadedMapper.getIntMap().put("key2", 40);
			database.save(loadedMapper);
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

			if (!d.field("key").equals(10) && !d.field("key").equals(40)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapUpdateItemInTxRollback() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		database.begin();
		final Mapper loadedMapper = (Mapper) database.load(new ORecordId(mapper.getId()));
		loadedMapper.getIntMap().put("key2", 40);
		database.save(loadedMapper);
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

			if (!d.field("key").equals(10) && !d.field("key").equals(20)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapRemoveItem() {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);
		map.put("key3", 30);

		mapper.setIntMap(map);
		database.save(mapper);

		database.command(new OCommandSQL("UPDATE " + mapper.getId() + " remove intMap = 'key2'")).execute();

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

			if (!d.field("key").equals(10) && !d.field("key").equals(30)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapRemoveItemInTx() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);
		map.put("key3", 30);

		mapper.setIntMap(map);
		database.save(mapper);

		try {
			database.begin();
			final Mapper loadedMapper = (Mapper)database.load(new ORecordId(mapper.getId()));
			loadedMapper.getIntMap().remove("key2");
			database.save(loadedMapper);
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

			if (!d.field("key").equals(10) && !d.field("key").equals(30)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapRemoveItemInTxRollback() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);
		map.put("key3", 30);

		mapper.setIntMap(map);
		database.save(mapper);


		database.begin();
		final Mapper loadedMapper = (Mapper) database.load(new ORecordId(mapper.getId()));
		loadedMapper.getIntMap().remove("key2");
		database.save(loadedMapper);
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

			if (!d.field("key").equals(10) && !d.field("key").equals(20) && !d.field("key").equals(30)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	
	public void testIndexMapRemove() {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);
		database.delete(mapper);

		final List<ODocument> resultByKey = database.command(new OCommandSQL("select key, rid from index:mapIndexTestKey")).execute();

		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 0);

		final List<ODocument> resultByValue = database.command(new OCommandSQL("select key, rid from index:mapIndexTestValue"))
				.execute();

		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 0);
	}

	public void testIndexMapRemoveInTx() throws Exception {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);
		
		try {
			database.begin();
			database.delete(mapper);
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
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		database.begin();
		database.delete(mapper);
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

			if (!d.field("key").equals(10) && !d.field("key").equals(20)) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	public void testIndexMapSQL() {
		final Mapper mapper = new Mapper();
		Map<String, Integer> map = new HashMap<String, Integer>();

		map.put("key1", 10);
		map.put("key2", 20);

		mapper.setIntMap(map);
		database.save(mapper);

		final List<Mapper> resultByKey = database.query(
				new OSQLSynchQuery<Collector>("select * from Mapper where intMap containskey ?"), "key1");
		Assert.assertNotNull(resultByKey);
		Assert.assertEquals(resultByKey.size(), 1);

		Assert.assertEquals(map, resultByKey.get(0).getIntMap());

		final List<Mapper> resultByValue = database.query(new OSQLSynchQuery<Collector>(
				"select * from Mapper where intMap containsvalue ?"), 10);
		Assert.assertNotNull(resultByValue);
		Assert.assertEquals(resultByValue.size(), 1);

		Assert.assertEquals(map, resultByValue.get(0).getIntMap());
	}
}
