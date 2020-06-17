package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that {@link ODocument} is serializable.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 12/20/12
 */
public class ODocumentSerializationPersistentTest {

  private static ODatabaseDocumentTx db;
  private static ORID docId;
  private static ORID linkedId;

  @BeforeClass
  public static void setUp() throws Exception {

    db = new ODatabaseDocumentTx("memory:testdocumentserialization");

    db.create();

    final ODocument doc = new ODocument();
    doc.field("name", "Artem");

    final ODocument linkedDoc = new ODocument();

    doc.field("country", linkedDoc);
    doc.field("numbers", Arrays.asList(0, 1, 2, 3, 4, 5));
    doc.save(db.getClusterNameById(db.getDefaultClusterId()));
    docId = doc.getIdentity();
    linkedId = linkedDoc.getIdentity();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testSerialization() throws Exception {
    final ODocument doc = db.load(docId);

    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream);

    out.writeObject(doc);
    db.close();

    final ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    final ODocument loadedDoc = (ODocument) in.readObject();

    assertEquals(loadedDoc.getIdentity(), docId);
    assertEquals(loadedDoc.getVersion(), doc.getVersion());
    assertEquals(loadedDoc.field("name"), "Artem");
    assertEquals(loadedDoc.field("country"), linkedId);

    final List<Integer> numbers = loadedDoc.field("numbers");
    for (int i = 0; i < numbers.size(); i++) {
      assertEquals((int) numbers.get(i), i);
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testRidBagInEmbeddedDocument() {
    ODatabaseRecordThreadLocal.instance().set(db);
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

    byte[] res = db.getSerializer().toStream(doc);
    db.getSerializer().fromStream(res, new ODocument(), new String[] {});
  }
}
