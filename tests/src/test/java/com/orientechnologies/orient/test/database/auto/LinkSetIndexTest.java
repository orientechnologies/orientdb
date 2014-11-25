package com.orientechnologies.orient.test.database.auto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 3/28/14
 */
public class LinkSetIndexTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public LinkSetIndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void setupSchema() {
    final OClass ridBagIndexTestClass = database.getMetadata().getSchema().createClass("LinkSetIndexTestClass");

    ridBagIndexTestClass.createProperty("linkSet", OType.LINKSET);

    ridBagIndexTestClass.createIndex("linkSetIndex", OClass.INDEX_TYPE.NOTUNIQUE, "linkSet");
    database.getMetadata().getSchema().save();
    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command(new OCommandSQL("DELETE FROM LinkSetIndexTestClass")).execute();

    List<ODocument> result = database.command(new OCommandSQL("select from LinkSetIndexTestClass")).execute();
    Assert.assertEquals(result.size(), 0);

    result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();
    Assert.assertEquals(result.size(), 0);

    database.close();
  }

  public void testIndexLinkSet() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    try {
      database.begin();
      final ODocument document = new ODocument("LinkSetIndexTestClass");
      final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
      linkSet.add(docOne);
      linkSet.add(docTwo);

      document.field("linkSet", linkSet);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdate() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<OIdentifiable>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    final Set<OIdentifiable> linkSetTwo = new HashSet<OIdentifiable>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    document.save();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<OIdentifiable>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    try {
      database.begin();

      final Set<OIdentifiable> linkSetTwo = new HashSet<OIdentifiable>();
      linkSetTwo.add(docOne);
      linkSetTwo.add(docThree);

      document.field("linkSet", linkSetTwo);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final Set<OIdentifiable> linkSetOne = new HashSet<OIdentifiable>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    document.field("linkSet", linkSetOne);
    document.save();

    database.begin();

    final Set<OIdentifiable> linkSetTwo = new HashSet<OIdentifiable>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    document.save();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateAddItem() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);

    document.save();

    database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " add linkSet = " + docThree.getIdentity())).execute();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateAddItemInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<OIdentifiable>> field("linkSet").add(docThree);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateAddItemInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<OIdentifiable>> field("linkSet").add(docThree);
    loadedDocument.save();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateRemoveItemInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();

    try {
      database.begin();
      ODocument loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<OIdentifiable>> field("linkSet").remove(docTwo);
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateRemoveItemInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();

    database.begin();
    ODocument loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<OIdentifiable>> field("linkSet").remove(docTwo);
    loadedDocument.save();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetUpdateRemoveItem() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    database.command(new OCommandSQL("UPDATE " + document.getIdentity() + " remove linkSet = " + docTwo.getIdentity())).execute();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetRemove() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");

    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    document.delete();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  public void testIndexLinkSetRemoveInTx() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");

    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    try {
      database.begin();
      document.delete();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 0);
  }

  public void testIndexLinkSetRemoveInTxRollback() throws Exception {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    database.begin();
    document.delete();
    database.rollback();

    List<ODocument> result = database.command(new OCommandSQL("select key, rid from index:linkSetIndex")).execute();

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

  public void testIndexLinkSetSQL() {
    final ODocument docOne = new ODocument();
    docOne.save();

    final ODocument docTwo = new ODocument();
    docTwo.save();

    final ODocument docThree = new ODocument();
    docThree.save();

    ODocument document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSetOne = new HashSet<OIdentifiable>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    document = new ODocument("LinkSetIndexTestClass");
    final Set<OIdentifiable> linkSet = new HashSet<OIdentifiable>();
    linkSet.add(docThree);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select * from LinkSetIndexTestClass where linkSet contains ?"), docOne.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 1);

    List<OIdentifiable> listResult = new ArrayList<OIdentifiable>();
    for (OIdentifiable identifiable : result.get(0).<Set<OIdentifiable>> field("linkSet"))
      listResult.add(identifiable);
    Assert.assertEquals(listResult.size(), 2);
    Assert.assertTrue(listResult.containsAll(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
  }
}
