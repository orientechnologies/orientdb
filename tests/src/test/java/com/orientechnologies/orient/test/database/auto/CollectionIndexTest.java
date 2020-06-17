/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.whiz.Collector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class CollectionIndexTest extends ObjectDBBaseTest {

  @Parameters(value = "url")
  public CollectionIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");

    final OClass collector = database.getMetadata().getSchema().getClass("Collector");
    collector.createProperty("id", OType.STRING);
    collector
        .createProperty("stringCollection", OType.EMBEDDEDLIST, OType.STRING)
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("delete from Collector")).execute();

    super.afterMethod();
  }

  public void testIndexCollection() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    database.save(collector);

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionInTx() {
    checkEmbeddedDB();

    try {
      database.begin();
      Collector collector = new Collector();
      collector.setStringCollection(Arrays.asList("spam", "eggs"));
      database.save(collector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();
      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdate() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    collector.setStringCollection(Arrays.asList("spam", "bacon"));
    database.save(collector);

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("bacon")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTx() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    try {
      database.begin();
      collector.setStringCollection(Arrays.asList("spam", "bacon"));
      database.save(collector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(), 2);
    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("bacon")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTxRollback() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    database.begin();
    collector.setStringCollection(Arrays.asList("spam", "bacon"));
    database.save(collector);
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItem() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);

    database
        .command(
            new OCommandSQL("UPDATE " + collector.getId() + " add stringCollection = 'cookies'"))
        .execute();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs") && !key.equals("cookies")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTx() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    try {
      database.begin();
      Collector loadedCollector = database.load(new ORecordId(collector.getId()));
      loadedCollector.getStringCollection().add("cookies");
      database.save(loadedCollector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs") && !key.equals("cookies")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    database.begin();
    Collector loadedCollector = database.load(new ORecordId(collector.getId()));
    loadedCollector.getStringCollection().add("cookies");
    database.save(loadedCollector);
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    try {
      database.begin();
      Collector loadedCollector = database.load(new ORecordId(collector.getId()));
      loadedCollector.getStringCollection().remove("spam");
      database.save(loadedCollector);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 1);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(new ArrayList<>(Arrays.asList("spam", "eggs")));
    collector = database.save(collector);

    database.begin();
    Collector loadedCollector = database.load(new ORecordId(collector.getId()));
    loadedCollector.getStringCollection().remove("spam");
    database.save(loadedCollector);
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItem() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);

    database
        .command(
            new OCommandSQL("UPDATE " + collector.getId() + " remove stringCollection = 'spam'"))
        .execute();

    final OIndex index = getIndex("Collector.stringCollection");

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionRemove() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    database.delete(collector);

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexCollectionRemoveInTx() {
    checkEmbeddedDB();

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

    final OIndex index = getIndex("Collector.stringCollection");

    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexCollectionRemoveInTxRollback() {
    checkEmbeddedDB();

    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    collector = database.save(collector);
    database.begin();
    database.delete(collector);
    database.rollback();

    final OIndex index = getIndex("Collector.stringCollection");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("spam") && !key.equals("eggs")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionSQL() {
    Collector collector = new Collector();
    collector.setStringCollection(Arrays.asList("spam", "eggs"));
    database.save(collector);

    List<Collector> result =
        database.query(
            new OSQLSynchQuery<Collector>(
                "select * from Collector where stringCollection contains ?"),
            "eggs");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(Arrays.asList("spam", "eggs"), result.get(0).getStringCollection());
  }
}
