package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1/30/14
 */
@Test(groups = {"index"})
public class LinkBagIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public LinkBagIndexTest(@Optional final String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass ridBagIndexTestClass =
        database.getMetadata().getSchema().createClass("RidBagIndexTestClass");

    ridBagIndexTestClass.createProperty("ridBag", OType.LINKBAG);

    ridBagIndexTestClass.createIndex("ridBagIndex", OClass.INDEX_TYPE.NOTUNIQUE, "ridBag");

    database.close();
  }

  @AfterClass
  public void destroySchema() {
    if (database.isClosed()) {
      reopendb("admin", "admin");
    }

    database.getMetadata().getSchema().dropClass("RidBagIndexTestClass");
    database.close();
  }

  @AfterMethod
  public void afterMethod() {
    database.command("DELETE FROM RidBagIndexTestClass").close();

    OResultSet result = database.query("select from RidBagIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (!database.getStorage().isRemote()) {
      final OIndex index = getIndex("ridBagIndex");
      Assert.assertEquals(index.getInternal().size(), 0);
    }
  }

  public void testIndexRidBag() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    try {
      database.begin();
      final ODocument document = new ODocument("RidBagIndexTestClass");
      final ORidBag ridBag = new ORidBag();
      ridBag.add(docOne);
      ridBag.add(docTwo);

      document.field("ridBag", ridBag);
      database.save(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdate() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    database.save(document);

    final ORidBag ridBagTwo = new ORidBag();
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document.field("ridBag", ridBagTwo);
    database.save(document);

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    database.save(document);

    try {
      database.begin();

      final ORidBag ridBagTwo = new ORidBag();
      ridBagTwo.add(docOne);
      ridBagTwo.add(docThree);

      document.field("ridBag", ridBagTwo);
      database.save(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    final ODocument document = new ODocument("RidBagIndexTestClass");
    document.field("ridBag", ridBagOne);
    database.save(document);

    database.begin();

    final ORidBag ridBagTwo = new ORidBag();
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document.field("ridBag", ridBagTwo);
    database.save(document);
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateAddItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);

    database.save(document);

    database
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set ridBag = ridBag || "
                + docThree.getIdentity())
        .close();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateAddItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<ORidBag>field("ridBag").add(docThree);
      database.save(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<ORidBag>field("ridBag").add(docThree);
    database.save(loadedDocument);
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(), 2);
    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);
    database.save(document);

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<ORidBag>field("ridBag").remove(docTwo);
      database.save(loadedDocument);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(), 1);
    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);
    database.save(document);

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<ORidBag>field("ridBag").remove(docTwo);
    database.save(loadedDocument);
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);

    //noinspection deprecation
    database
        .command("UPDATE " + document.getIdentity() + " remove ridBag = " + docTwo.getIdentity())
        .close();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 1);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagRemove() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");

    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);
    database.delete(document);

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexRidBagRemoveInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");

    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);
    try {
      database.begin();
      database.delete(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexRidBagRemoveInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);

    database.begin();
    database.delete(document);
    database.rollback();

    final OIndex index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagSQL() {
    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    database.save(document);

    document = new ODocument("RidBagIndexTestClass");
    ORidBag ridBag = new ORidBag();
    ridBag.add(docThree);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    database.save(document);

    OResultSet result =
        database.query(
            "select * from RidBagIndexTestClass where ridBag contains ?", docOne.getIdentity());
    OResult res = result.next();

    List<OIdentifiable> listResult = new ArrayList<>();
    for (OIdentifiable identifiable : res.<ORidBag>getProperty("ridBag"))
      listResult.add(identifiable);
    result.close();

    Assert.assertEquals(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()), listResult);
  }
}
