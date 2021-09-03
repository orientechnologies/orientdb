package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @since 22.03.12 */
@SuppressWarnings("deprecation")
@Test(groups = {"index"})
public class LinkMapIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public LinkMapIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass linkMapIndexTestClass =
        database.getMetadata().getSchema().createClass("LinkMapIndexTestClass");
    linkMapIndexTestClass.createProperty("linkMap", OType.LINKMAP);

    linkMapIndexTestClass.createIndex("mapIndexTestKey", OClass.INDEX_TYPE.NOTUNIQUE, "linkMap");
    linkMapIndexTestClass.createIndex(
        "mapIndexTestValue", OClass.INDEX_TYPE.NOTUNIQUE, "linkMap by value");
  }

  @AfterClass
  public void destroySchema() {
    //noinspection deprecation
    database.open("admin", "admin");
    database.getMetadata().getSchema().dropClass("LinkMapIndexTestClass");
    database.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("delete from LinkMapIndexTestClass")).execute();

    super.afterMethod();
  }

  public void testIndexMap() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();

        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");

    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);
    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    try {
      database.begin();
      Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOne() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> mapOne = new HashMap<>();

    mapOne.put("key1", docOne.getIdentity());
    mapOne.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", mapOne);
    document.save();

    final Map<String, ORID> mapTwo = new HashMap<>();
    mapTwo.put("key2", docOne.getIdentity());
    mapTwo.put("key3", docThree.getIdentity());

    document.field("linkMap", mapTwo);
    document.save();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    database.begin();
    try {
      final Map<String, ORID> mapTwo = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateOneTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> mapOne = new HashMap<>();

    mapOne.put("key1", docOne.getIdentity());
    mapOne.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", mapOne);
    document.save();

    database.begin();
    final Map<String, ORID> mapTwo = new HashMap<>();

    mapTwo.put("key3", docTwo.getIdentity());
    mapTwo.put("key2", docThree.getIdentity());

    document.field("linkMap", mapTwo);
    document.save();
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key2") && !key.equals("key1")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docTwo.getIdentity())
            && !value.equals(docOne.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();

    database
        .command(
            new OCommandSQL(
                "UPDATE "
                    + document.getIdentity()
                    + " put linkMap = 'key3', "
                    + docThree.getIdentity()))
        .execute();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 3);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 3);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();

        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 3);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapAddItemTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();

    database
        .command(
            new OCommandSQL(
                "UPDATE "
                    + document.getIdentity()
                    + " put linkMap = 'key2',"
                    + docThree.getIdentity()))
        .execute();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");

    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);
    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);
    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + value);
        }
      }
    }
  }

  public void testIndexMapUpdateItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());
    map.put("key3", docThree.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();

    database
        .command(new OCommandSQL("UPDATE " + document.getIdentity() + " remove linkMap = 'key2'"))
        .execute();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemoveItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        String key = (String) keyIterator.next();
        if (!key.equals("key1") && !key.equals("key2") && !key.equals("key3")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 3);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())
            && !value.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapRemove() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();
    document.delete();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 0);

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 0);
  }

  public void testIndexMapRemoveInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

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

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 0);

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 0);
  }

  public void testIndexMapRemoveInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();

    database.begin();
    document.delete();
    database.rollback();

    final OIndex keyIndexMap = getIndex("mapIndexTestKey");
    Assert.assertEquals(keyIndexMap.getInternal().size(), 2);

    final Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = keyIndexMap.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        String key = (String) keysIterator.next();
        if (!key.equals("key1") && !key.equals("key2")) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }

    final OIndex valueIndexMap = getIndex("mapIndexTestValue");
    Assert.assertEquals(valueIndexMap.getInternal().size(), 2);

    final Iterator<Object> valuesIterator;
    try (Stream<Object> valueStream = valueIndexMap.getInternal().keyStream()) {
      valuesIterator = valueStream.iterator();

      while (valuesIterator.hasNext()) {
        OIdentifiable value = (OIdentifiable) valuesIterator.next();
        if (!value.getIdentity().equals(docOne.getIdentity())
            && !value.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown value found: " + value);
        }
      }
    }
  }

  public void testIndexMapSQL() {
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    Map<String, ORID> map = new HashMap<>();

    map.put("key1", docOne.getIdentity());
    map.put("key2", docTwo.getIdentity());

    final ODocument document = new ODocument("LinkMapIndexTestClass");
    document.field("linkMap", map);
    document.save();

    final List<ODocument> resultByKey =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select * from LinkMapIndexTestClass where linkMap containskey ?"),
            "key1");
    Assert.assertNotNull(resultByKey);
    Assert.assertEquals(resultByKey.size(), 1);

    Assert.assertEquals(map, document.field("linkMap"));

    final List<ODocument> resultByValue =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select * from LinkMapIndexTestClass where linkMap  containsvalue ?"),
            docOne.getIdentity());
    Assert.assertNotNull(resultByValue);
    Assert.assertEquals(resultByValue.size(), 1);

    Assert.assertEquals(map, document.field("linkMap"));
  }
}
