/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.domain.whiz.Collector;

@Test(groups = { "index" })
public class CollectionIndexTest {
  private final OObjectDatabaseTx database;

  @Parameters(value = "url")
  public CollectionIndexTest(final String iURL) {
    database = new OObjectDatabaseTx(iURL);
  }

  @BeforeClass
  public void setupSchema() {
    database.open("admin", "admin");
    final OClass collector = database.getMetadata().getSchema().getClass("Collector");
    collector.createProperty("id", OType.STRING);
    collector.createProperty("stringCollection", OType.EMBEDDEDLIST, OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

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

  public void testIndexCollection() {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);

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

  public void testIndexCollectionInTx() throws Exception {
    try {
      database.begin();
      Collector collector = new Collector();
      collector.setStringCollection(Arrays.asList("spam", "eggs"));
      collector = database.save(collector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

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

  public void testIndexCollectionUpdate() {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    collector.setStringCollection(Arrays.asList("spam", "bacon"));
    collector = database.save(collector);

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

  public void testIndexCollectionUpdateInTx() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    try {
      database.begin();
      collector.setStringCollection(Arrays.asList("spam", "bacon"));
      collector = database.save(collector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

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

  public void testIndexCollectionUpdateInTxRollback() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    database.begin();
    collector.setStringCollection(Arrays.asList("spam", "bacon"));
    collector = database.save(collector);
    database.rollback();

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

  public void testIndexCollectionUpdateAddItem() {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);

    database.command(new OCommandSQL("UPDATE " + collector.getId() + " add stringCollection = 'cookies'")).execute();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals("spam") && !d.field("key").equals("eggs") && !d.field("key").equals("cookies")) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTx() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<String>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    try {
      database.begin();
      Collector loadedCollector = (Collector) database.load(new ORecordId(collector.getId()));
      loadedCollector.getStringCollection().add("cookies");
      database.save(loadedCollector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals("spam") && !d.field("key").equals("eggs") && !d.field("key").equals("cookies")) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTxRollback() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<String>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    database.begin();
    Collector loadedCollector = (Collector) database.load(new ORecordId(collector.getId()));
    loadedCollector.getStringCollection().add("cookies");
    loadedCollector = database.save(loadedCollector);
    database.rollback();

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

  public void testIndexCollectionUpdateRemoveItemInTx() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<String>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    try {
      database.begin();
      Collector loadedCollector = (Collector) database.load(new ORecordId(collector.getId()));
      loadedCollector.getStringCollection().remove("spam");
      loadedCollector = database.save(loadedCollector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals("eggs")) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTxRollback() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<String>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    database.begin();
    Collector loadedCollector = (Collector) database.load(new ORecordId(collector.getId()));
    loadedCollector.getStringCollection().remove("spam");
    loadedCollector = database.save(loadedCollector);
    database.rollback();

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

  public void testIndexCollectionUpdateRemoveItem() {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);

    database.command(new OCommandSQL("UPDATE " + collector.getId() + " remove stringCollection = 'spam'")).execute();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals("eggs")) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexCollectionRemove() {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    database.delete(collector);

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  public void testIndexCollectionRemoveInTx() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    try {
      database.begin();
      database.delete(collector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:Collector.stringCollection")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  public void testIndexCollectionRemoveInTxRollback() throws Exception {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    database.begin();
    database.delete(collector);
    database.rollback();

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

  public void testIndexCollectionSQL() {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);

    List<Collector> result = database.query(new OSQLSynchQuery<Collector>(
        "select * from Collector where stringCollection contains ?"), "eggs");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(Arrays.asList("spam", "eggs"), result.get(0).getStringCollection());
  }
}
