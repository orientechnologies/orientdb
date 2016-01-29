package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created by tglman on 26/10/15.
 */
public class TestRecursiveLinkedSave {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + TestRecursiveLinkedSave.class.getSimpleName());
    db.create();
  }

  @Test
  public void testLinked() {
    db.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    ODocument doc1 = new ODocument("Test");
    doc.field("link", doc1);
    ODocument doc2 = new ODocument("Test");
    doc1.field("link", doc2);
    doc2.field("link", doc);
    doc = db.save(doc);
    assertEquals(3, db.countClass("Test"));
    doc = db.load(doc.getIdentity());
    doc1 = doc.field("link");
    doc2 = doc1.field("link");
    assertEquals(doc, doc2.field("link"));
  }

  @Test
  public void testTxLinked() {
    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    ODocument doc = new ODocument("Test");
    ODocument doc1 = new ODocument("Test");
    doc.field("link", doc1);
    ODocument doc2 = new ODocument("Test");
    doc1.field("link", doc2);
    doc2.field("link", doc);
    db.save(doc);
    db.commit();
    assertEquals(3, db.countClass("Test"));
    doc = db.load(doc.getIdentity());
    doc1 = doc.field("link");
    doc2 = doc1.field("link");
    assertEquals(doc, doc2.field("link"));
  }

  @AfterMethod
  public void after() {
    db.drop();
  }

}
