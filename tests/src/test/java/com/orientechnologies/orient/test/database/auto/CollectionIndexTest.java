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

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Collector;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.Arrays;
import java.util.List;

@Test(groups = {"index"})
public class CollectionIndexTest {
	private final ODatabaseObjectTx database;

    @Parameters(value = "url")
    public CollectionIndexTest(final String iURL) {
        database = new ODatabaseObjectTx(iURL);
        database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
    }

	@BeforeClass
	public void setupSchema() {
        database.open("admin", "admin");
		final OClass collector = database.getMetadata().getSchema().createClass("Collector");
		collector.createProperty("id", OType.STRING);
		collector.createProperty("stringCollection",
                OType.EMBEDDEDLIST, OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

        database.getMetadata().getSchema().save();
		database.close();
	}

	@AfterClass
	public void destroySchema() {
		database.open("admin", "admin");
		database.getMetadata().getSchema().dropClass("Collector");
		database.close();
	}

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() throws Exception {
		database.command(new OCommandSQL("delete from Collector")).execute();
		database.close();
	}

	@Test
	public void testIndexCollection() {
		Collector collector = new Collector();
		collector.setStringCollection(Arrays.asList("spam", "eggs"));
		database.save(collector);

		List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 2);
		for (ODocument d : result) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("spam") && !d.field("key").equals("eggs")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	@Test
	public void testIndexCollectionUpdate() {
		Collector collector = new Collector();
		collector.setStringCollection(Arrays.asList("spam", "eggs"));
		database.save(collector);
		collector.setStringCollection(Arrays.asList("spam", "bacon"));
		database.save(collector);

		List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 2);
		for (ODocument d : result) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (!d.field("key").equals("spam") && !d.field("key").equals("bacon")) {
				Assert.fail("Unknown key found: " + d.field("key"));
			}
		}
	}

	@Test
	public void testIndexCollectionRemove() {
		Collector collector = new Collector();
		collector.setStringCollection(Arrays.asList("spam", "eggs"));
		database.save(collector);
		database.delete(collector);

		List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 0);
	}

	@Test
	public void testIndexCollectionSQL() {
		Collector collector = new Collector();
		collector.setStringCollection(Arrays.asList("spam", "eggs"));
		database.save(collector);

		List<Collector> result = database.query(new OSQLSynchQuery<Collector>("select * from Collector where stringCollection contains ?"), "eggs");
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);
		Assert.assertEquals(Arrays.asList("spam", "eggs"), result.get(0).getStringCollection());
	}
}
