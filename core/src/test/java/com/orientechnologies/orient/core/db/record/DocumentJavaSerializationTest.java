package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DocumentJavaSerializationTest {
  private String previousSerializerConf;
  private ORecordSerializer previousSerializerInstance;

  @Before
  public void before() {
    this.previousSerializerConf = OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString();
    previousSerializerInstance = ODatabaseDocumentTx.getDefaultSerializer();
  }

  @After
  public void after() {
    OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.setValue(this.previousSerializerConf);
    ODatabaseDocumentTx.setDefaultSerializer(previousSerializerInstance);
  }

  @Test
  public void testSimpleSerialization() throws IOException, ClassNotFoundException {
    ODocument doc = new ODocument();
    doc.field("one", "one");
    doc.field("two", "two");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(doc);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    ODocument doc1 = (ODocument) ois.readObject();
    assertEquals("one", doc1.field("one"));
    assertEquals("two", doc1.field("two"));
  }

  @Test
  public void testCsvBinarySerialization() throws IOException, ClassNotFoundException {
    OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.setValue(ORecordSerializerSchemaAware2CSV.NAME);
    ODatabaseDocumentTx.setDefaultSerializer(
        ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME));
    ODocument doc = new ODocument();
    ORecordInternal.setRecordSerializer(
        doc, ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME));
    doc.field("one", "one");
    doc.field("two", "two");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(doc);

    OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.setValue(ORecordSerializerBinary.NAME);
    ODatabaseDocumentTx.setDefaultSerializer(
        ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    ODocument doc1 = (ODocument) ois.readObject();
    assertEquals("one", doc1.field("one"));
    assertEquals("two", doc1.field("two"));
  }

  @Test
  public void testBinaryCsvSerialization() throws IOException, ClassNotFoundException {
    OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.setValue(ORecordSerializerBinary.NAME);
    ODatabaseDocumentTx.setDefaultSerializer(
        ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));
    ODocument doc = new ODocument();
    ORecordInternal.setRecordSerializer(
        doc, ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));
    doc.field("one", "one");
    doc.field("two", "two");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(doc);

    OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.setValue(ORecordSerializerSchemaAware2CSV.NAME);
    ODatabaseDocumentTx.setDefaultSerializer(
        ORecordSerializerFactory.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME));

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(bais);
    ODocument doc1 = (ODocument) ois.readObject();
    assertEquals("one", doc1.field("one"));
    assertEquals("two", doc1.field("two"));
  }
}
