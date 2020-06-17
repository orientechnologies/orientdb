package com.orientechnologies.orient.core.record;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

/** Created by tglman on 24/06/16. */
public class DocumentIndependentJavaSerializationTest {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    ODatabaseDocument db =
        new ODatabaseDocumentTx(
            "memory:" + DocumentIndependentJavaSerializationTest.class.getSimpleName());
    db.create();
    byte[] ser;
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty("test", OType.STRING);
      ODocument doc = new ODocument(clazz);
      doc.field("test", "Some Value");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(doc);
      ser = baos.toByteArray();
    } finally {
      db.drop();
    }

    ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(ser));
    ODocument doc = (ODocument) input.readObject();

    assertEquals(doc.getClassName(), "Test");
    assertEquals(doc.field("test"), "Some Value");
  }

  @Test
  public void testDeserializationSave() throws IOException, ClassNotFoundException {
    ODocument doc = new ODocument("Test");
    doc.field("test", "Some Value");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(doc);
    byte[] ser = baos.toByteArray();

    ODatabaseDocumentInternal db =
        new ODatabaseDocumentTx(
            "memory:" + DocumentIndependentJavaSerializationTest.class.getSimpleName());
    db.create();

    try {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.createProperty("test", OType.STRING);
      ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(ser));
      ODocument doc1 = (ODocument) input.readObject();
      assertEquals(doc1.recordFormat, db.getSerializer());
      assertEquals(doc1.getClassName(), "Test");
      assertEquals(doc1.field("test"), "Some Value");
    } finally {
      db.drop();
    }
  }
}
