package com.orientechnologies.orient.core.record.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Tests that {@link ODocument} is serializable.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @since 12/20/12
 */
public class ODocumentSerializationPersistentTest {

  private ODatabaseDocumentTx db;
  private ORID                docId;
  private ORID                linkedId;

  @BeforeMethod
  public void setUp() throws Exception {
    OGlobalConfiguration.DB_USE_DISTRIBUTED_VERSION.setValue(Boolean.TRUE);

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
    Assert.assertEquals(loadedDoc.field("country", OType.LINK), linkedId);

    final List<Integer> numbers = loadedDoc.field("numbers");
    for (int i = 0; i < numbers.size(); i++) {
      Assert.assertEquals((int) numbers.get(i), i);
    }
  }
}
