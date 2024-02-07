package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
 * @since 21.03.12
 */
@Test(groups = {"index"})
public class LinkListIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public LinkListIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass linkListIndexTestClass =
        database.getMetadata().getSchema().createClass("LinkListIndexTestClass");

    linkListIndexTestClass.createProperty("linkCollection", OType.LINKLIST);

    linkListIndexTestClass.createIndex(
        "linkCollectionIndex", OClass.INDEX_TYPE.NOTUNIQUE, "linkCollection");
  }

  @AfterClass
  public void destroySchema() {
    //noinspection deprecation
    database.open("admin", "admin");
    database.getMetadata().getSchema().dropClass("LinkListIndexTestClass");
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command("DELETE FROM LinkListIndexTestClass").close();

    OResultSet result = database.query("select from LinkListIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (!database.getStorage().isRemote()) {
      final OIndex index = getIndex("linkCollectionIndex");
      Assert.assertEquals(index.getInternal().size(), 0);
    }

    super.afterMethod();
  }

  public void testIndexCollection() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    try {
      database.begin();
      final ODocument document = new ODocument("LinkListIndexTestClass");
      document.field(
          "linkCollection",
          new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity()) && !key.equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdate() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
    document.save();

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    try {
      database.begin();
      document.field(
          "linkCollection",
          new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    database.begin();
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
    document.save();
    database.rollback();

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    database
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set linkCollection = linkCollection || "
                + docThree.getIdentity())
        .close();

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 3);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

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

  public void testIndexCollectionUpdateAddItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<List<OIdentifiable>>field("linkCollection").add(docThree.getIdentity());
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 3);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

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

  public void testIndexCollectionUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docThree = new ODocument();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<List<OIdentifiable>>field("linkCollection").add(docThree.getIdentity());
    loadedDocument.save();
    database.rollback();

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<List>field("linkCollection").remove(docTwo.getIdentity());
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 1);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<List>field("linkCollection").remove(docTwo.getIdentity());
    loadedDocument.save();
    database.rollback();

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItem() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    database.command(
        "UPDATE " + document.getIdentity() + " remove linkCollection = " + docTwo.getIdentity());

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 1);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionRemove() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    document.delete();

    OIndex index = getIndex("linkCollectionIndex");

    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexCollectionRemoveInTx() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    try {
      database.begin();
      document.delete();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testIndexCollectionRemoveInTxRollback() {
    checkEmbeddedDB();

    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    database.begin();
    document.delete();
    database.rollback();

    OIndex index = getIndex("linkCollectionIndex");

    Assert.assertEquals(index.getInternal().size(), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        OIdentifiable key = (OIdentifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionSQL() {
    final ODocument docOne = new ODocument();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument docTwo = new ODocument();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument document = new ODocument("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    OResultSet result =
        database.query(
            "select * from LinkListIndexTestClass where linkCollection contains ?",
            docOne.getIdentity());
    Assert.assertEquals(
        Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()),
        result.next().<List>getProperty("linkCollection"));
  }
}
