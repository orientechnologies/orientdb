package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 1/30/14
 */
@Test(groups = { "index" })
public class LinkBagIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public LinkBagIndexTest(@Optional final String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass ridBagIndexTestClass = database.getMetadata().getSchema().createClass("RidBagIndexTestClass");

    ridBagIndexTestClass.createProperty("ridBag", OType.LINKBAG);

    ridBagIndexTestClass.createIndex("ridBagIndex", OClass.INDEX_TYPE.NOTUNIQUE, "ridBag");

    database.close();
  }

  @AfterClass
  public void destroySchema() {
    if (database.isClosed())
      database.open("admin", "admin");

    database.getMetadata().getSchema().dropClass("RidBagIndexTestClass");
    database.close();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("DELETE FROM RidBagIndexTestClass")).execute();

    List<ODocument> result = database.command(new OCommandSQL("select from RidBagIndexTestClass")).execute();
    Assert.assertEquals(result.size(), 0);

    result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();
    Assert.assertEquals(result.size(), 0);
  }

  public void testIndexRidBag() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    try {
      database.begin();
      final ODocument document = new ODocument("RidBagIndexTestClass");
      final ORidBag ridBag = new ORidBag();
      ridBag.add(docOne);
      ridBag.add(docTwo);

      document.field("ridBag", ridBag);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdate() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    document.save();

    final ORidBag ridBagTwo = new ORidBag();
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document.field("ridBag", ridBagTwo);
    document.save();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    document.save();

    try {
      database.begin();

      final ORidBag ridBagTwo = new ORidBag();
      ridBagTwo.add(docOne);
      ridBagTwo.add(docThree);

      document.field("ridBag", ridBagTwo);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    final ODocument document = new ODocument("RidBagIndexTestClass");
    document.field("ridBag", ridBagOne);
    document.save();

    database.begin();

    final ORidBag ridBagTwo = new ORidBag();
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document.field("ridBag", ridBagTwo);
    document.save();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateAddItem() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);

    document.save();

    database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " add ridBag = " + docThree.getIdentity())).execute();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())
          && !d.field("key").equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateAddItemInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<ORidBag> field("ridBag").add(docThree);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 3);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())
          && !d.field("key").equals(docThree.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateAddItemInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<ORidBag> field("ridBag").add(docThree);
    loadedDocument.save();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<ORidBag> field("ridBag").remove(docTwo);
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<ORidBag> field("ridBag").remove(docTwo);
    loadedDocument.save();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItem() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();

    database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " remove ridBag = " + docTwo.getIdentity())).execute();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagRemove() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");

    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    document.delete();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  public void testIndexRidBagRemoveInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");

    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    try {
      database.begin();
      document.delete();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  public void testIndexRidBagRemoveInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBag = new ORidBag();
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();

    database.begin();
    document.delete();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:ridBagIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (!d.field("key").equals(docOne.getIdentity()) && !d.field("key").equals(docTwo.getIdentity())) {
        Assert.fail("Unknown key found: " + d.field("key"));
      }
    }
  }

  public void testIndexRidBagSQL() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    ODocument document = new ODocument("RidBagIndexTestClass");
    final ORidBag ridBagOne = new ORidBag();
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    document.save();

    document = new ODocument("RidBagIndexTestClass");
    ORidBag ridBag = new ORidBag();
    ridBag.add(docThree);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select * from RidBagIndexTestClass where ridBag contains ?"), docOne.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);

    List<OIdentifiable> listResult = new ArrayList<OIdentifiable>();
    for (OIdentifiable identifiable : result.get(0).<ORidBag> field("ridBag"))
      listResult.add(identifiable);

    Assert.assertEquals(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()), listResult);
  }
}
