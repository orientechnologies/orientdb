package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 3/28/14
 */
@SuppressWarnings("deprecation")
public class LinkSetIndexTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public LinkSetIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass ridBagIndexTestClass =
        database.getMetadata().getSchema().createClass("LinkSetIndexTestClass");

    ridBagIndexTestClass.createProperty("linkSet", OType.LINKSET);

    ridBagIndexTestClass.createIndex("linkSetIndex", OClass.INDEX_TYPE.NOTUNIQUE, "linkSet");
    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    reopendb("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() {
    checkEmbeddedDB();

    database.command("DELETE FROM LinkSetIndexTestClass").close();

    OResultSet result = database.command("select from LinkSetIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (database.isRemote()) {
      OIndex index =
          database.getMetadata().getIndexManagerInternal().getIndex(database, "linkSetIndex");
      Assert.assertEquals(index.getInternal().size(), 0);
    }

    database.close();
  }

  public void testIndexLinkSet() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    try {
      database.begin();
      final ODocument document = new ODocument("LinkSetIndexTestClass");
      final Set<OIdentifiable> linkSet = new HashSet<>();
      linkSet.add(docOne);
      linkSet.add(docTwo);

      document.field("linkSet", linkSet);
      database.save(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdate() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    database.save(document);

    final Set<OIdentifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    database.save(document);

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    database.save(document);

    try {
      database.begin();

      final Set<OIdentifiable> linkSetTwo = new HashSet<>();
      linkSetTwo.add(docOne);
      linkSetTwo.add(docThree);

      document.field("linkSet", linkSetTwo);
      database.save(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    document.field("linkSet", linkSetOne);
    database.save(document);

    database.begin();

    final Set<OIdentifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    database.save(document);
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateAddItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);

    database.save(document);

    database
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set linkSet = linkSet || "
                + docThree.getIdentity())
        .close();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateAddItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<OIdentifiable>>field("linkSet").add(docThree);
      database.save(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<OIdentifiable>>field("linkSet").add(docThree);
    database.save(loadedDocument);
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    database.save(document);

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<OIdentifiable>>field("linkSet").remove(docTwo);
      database.save(loadedDocument);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 1);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    database.save(document);

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<OIdentifiable>>field("linkSet").remove(docTwo);
    database.save(loadedDocument);
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);

    database
        .command("UPDATE " + document.getIdentity() + " remove linkSet = " + docTwo.getIdentity())
        .close();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 1);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetRemove() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");

    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);
    database.delete(document);

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexLinkSetRemoveInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");

    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);
    try {
      database.begin();
      database.delete(document);
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexLinkSetRemoveInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();

    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);

    database.begin();
    database.delete(document);
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetSQL() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    database.save(docOne, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    database.save(docTwo, database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    database.save(docThree, database.getClusterNameById(database.getDefaultClusterId()));

    ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    database.save(document);

    document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docThree);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    database.save(document);

    OResultSet result =
        database.query(
            "select * from LinkSetIndexTestClass where linkSet contains ?", docOne.getIdentity());

    List<OIdentifiable> listResult =
        new ArrayList<>(result.next().<Set<OIdentifiable>>getProperty("linkSet"));
    Assert.assertEquals(listResult.size(), 2);
    Assert.assertTrue(
        listResult.containsAll(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
  }
}
