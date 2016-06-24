package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 24/06/16.
 */
public class DocumentIndependentJavaSerializationTest {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + DocumentIndependentJavaSerializationTest.class.getSimpleName());
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

}
