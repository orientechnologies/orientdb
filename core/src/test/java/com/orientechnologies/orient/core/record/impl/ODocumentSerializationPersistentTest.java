package com.orientechnologies.orient.core.record.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Tests that {@link ODocument} is serializable.
 * 
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 12/20/12
 */
public class ODocumentSerializationPersistentTest {

  private ODatabaseDocumentTx db;
  private ORID                docId;
  private ORID                linkedId;

  @BeforeClass
  public void setUp() throws Exception {

    db = new ODatabaseDocumentTx("memory:testdocumentserialization");
    db.create();

    final ODocument doc = new ODocument();
    doc.field("name", "Artem");

    final ODocument linkedDoc = new ODocument();

    doc.field("country", linkedDoc);
    doc.field("numbers", Arrays.asList(0, 1, 2, 3, 4, 5));
    doc.save();
    docId = doc.getIdentity();
    linkedId = linkedDoc.getIdentity();
  }

  @Test
  public void testSerialization() throws Exception {
    final ODocument doc = db.load(docId);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream);

    out.writeObject(doc);
    db.close();

    final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    final ODocument loadedDoc = (ODocument) in.readObject();

    Assert.assertEquals(loadedDoc.getIdentity(), docId);
    Assert.assertEquals(loadedDoc.getRecordVersion(), doc.getRecordVersion());
    Assert.assertEquals(loadedDoc.field("name"), "Artem");
    Assert.assertEquals(loadedDoc.field("country"), linkedId);

    final List<Integer> numbers = loadedDoc.field("numbers");
    for (int i = 0; i < numbers.size(); i++) {
      Assert.assertEquals((int) numbers.get(i), i);
    }
  }

  @Test
  public void testRidBagInEmbeddedDocument() {
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    ODocument doc = new ODocument();
    ORidBag rids = new ORidBag();
    rids.add(new ORecordId(2, 3));
    rids.add(new ORecordId(2, 4));
    rids.add(new ORecordId(2, 5));
    rids.add(new ORecordId(2, 6));
    List<ODocument> docs = new ArrayList<ODocument>();
    ODocument doc1 = new ODocument();
    doc1.field("rids", rids);
    docs.add(doc1);
    ODocument doc2 = new ODocument();
    doc2.field("text", "text");
    docs.add(doc2);
    doc.field("emb", docs, OType.EMBEDDEDLIST);
    doc.field("some", "test");

    byte[] res = db.getSerializer().toStream(doc, false);
    ODocument extr = (ODocument) db.getSerializer().fromStream(res, new ODocument(), new String[] {});

    List<ODocument> emb = extr.field("emb");
    assertNotNull(emb);
    assertEquals(((ORidBag) emb.get(0).field("rids")).size(), rids.size());
    assertEquals(emb.get(1).field("text"), doc2.field("text"));
    assertEquals(extr.field("name"), doc.field("name"));

  }
}
