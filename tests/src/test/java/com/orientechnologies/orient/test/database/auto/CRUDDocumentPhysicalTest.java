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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
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
		database.open("admin", "admin");

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

			Assert.assertEquals(rec.field("binary"), base64);

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

		record.reset();

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
	public void testUpdateLazyDirtyPropagation() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		for (ODocument rec : database.browseCluster("Profile")) {
			Assert.assertFalse(rec.isDirty());

			List<?> followers = rec.field("followers");
			if (followers != null && followers.size() > 0) {
				followers.remove(followers.size() - 1);
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
		database.open("admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = ? and surname = ?");
		List<ODocument> result = database.command(query).execute("Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithPositionalParameters() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
		database.open("admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = ? and surname = ?");
		List<ODocument> result = database.query(query, "Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void commandWithNamedParameters() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
		database.open("admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<ODocument> result = database.command(query).execute(params);

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithNamedParameters() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
		database.open("admin", "admin");

		final OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<ODocument> result = database.query(query, params);

		Assert.assertTrue(result.size() != 0);

		database.close();
	}
}
