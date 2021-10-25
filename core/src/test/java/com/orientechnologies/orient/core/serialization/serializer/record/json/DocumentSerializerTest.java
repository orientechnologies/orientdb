package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.json.vserializers.DocumentSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class DocumentSerializerTest {
  private OrientDB orientDB;
  private ODatabaseSession session;

  @Before
  public void before() {
    final OrientDBConfig config =
        OrientDBConfig.builder().addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, true).build();
    orientDB = new OrientDB("memory:target/documentSerializerTest", "admin", "admin", config);
    orientDB.create("test", ODatabaseType.MEMORY);

    session = orientDB.open("test", "admin", "admin");
  }

  @After
  public void after() {
    session.close();
    orientDB.drop("test");
    orientDB.close();
  }

  @Test
  public void serializeEmptyDocument() throws Exception {
    final JsonFactory factory = new JsonFactory();
    final StringWriter writer = new StringWriter();

    final JsonGenerator generator = factory.createGenerator(writer);

    final ODocument document = new ODocument("TestClass");
    ORecordInternal.setVersion(document, 42);
    ORecordInternal.setIdentity(document, new ORecordId(24, 34));

    final DocumentSerializer serializer = DocumentSerializer.INSTANCE;

    serializer.toJSON(generator, document);
    generator.close();

    final String json = writer.toString();

    final JsonParser parser = factory.createParser(json);

    final ODocument resultedDocument = (ODocument) serializer.fromJSON(parser, null);

    final ODatabaseDocumentInternal sessionInternal = (ODatabaseDocumentInternal) session;

    assertDocumentsAreTheSame(document, resultedDocument, sessionInternal);
  }

  @Test
  public void serializePrimitiveFields() throws Exception {
    testSerializationDeserialization(this::generatePrimitiveTypes);
  }

  @Test
  public void serializeLinkedCollections() throws Exception {
    testSerializationDeserialization(this::generateLinkedCollections);
  }

  private void generateLinkedCollections(ODocument document) {
    final List<ORID> linkedList = new ArrayList<>();

    linkedList.add(new ORecordId(12, 56));
    linkedList.add(new ORecordId(10, 25));
    linkedList.add(new ORecordId(47, 78));

    final Map<String, ORID> linkedMap = new HashMap<>();

    linkedMap.put("key1", new ORecordId(56, 9));
    linkedMap.put("key2", new ORecordId(6, 93));
    linkedMap.put("key3", new ORecordId(12, 37));
    linkedMap.put("key4", new ORecordId(21, 42));

    final Set<ORID> linkedSet = new HashSet<>();
    linkedSet.add(new ORecordId(67, 89));
    linkedSet.add(new ORecordId(21, 98));
    linkedSet.add(new ORecordId(43, 112));

    document.field("linkedMap", linkedMap, OType.LINKMAP);
    document.field("linkedList", linkedList, OType.LINKLIST);
    document.field("linkedSet", linkedSet, OType.LINKSET);

    final ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag, OType.LINKBAG);

    ridBag.add(new ORecordId(12, 45));
    ridBag.add(new ORecordId(21, 54));
    ridBag.add(new ORecordId(121, 5));
  }

  private void generatePrimitiveTypes(ODocument document) {
    document.field("sVal", 3, OType.SHORT);
    document.field("iVal", 2148, OType.INTEGER);
    document.field("lVal", 547453452L, OType.LONG);
    document.field("strVal", "val", OType.STRING);
    document.field("fVal", 12.1f, OType.FLOAT);
    document.field("dVal", 43.34f, OType.DOUBLE);
    document.field("bVal", 42, OType.BYTE);
    document.field("boolVal", true, OType.BOOLEAN);
    document.field("linkVal", new ORecordId(12, 56), OType.LINK);
    document.field("binaryVal", new byte[] {12, 54, 67, 89}, OType.BINARY);
    document.field("decVal", new BigDecimal("13241235467.960464"), OType.DECIMAL);

    final Calendar dtCalendar = Calendar.getInstance();
    dtCalendar.set(2021, Calendar.OCTOBER, 23, 23, 10, 24);

    document.field("dtVal", dtCalendar.getTime(), OType.DATETIME);
    dtCalendar.set(2020, Calendar.NOVEMBER, 12);

    document.field("dateVal", dtCalendar.getTime(), OType.DATE);
  }

  private void testSerializationDeserialization(final Consumer<ODocument> consumer)
      throws IOException {
    final JsonFactory factory = new JsonFactory();
    final StringWriter writer = new StringWriter();

    final JsonGenerator generator = factory.createGenerator(writer);

    final ODocument document = new ODocument("TestClass");
    ORecordInternal.setVersion(document, 42);
    ORecordInternal.setIdentity(document, new ORecordId(24, 34));

    final DocumentSerializer serializer = DocumentSerializer.INSTANCE;

    consumer.accept(document);

    serializer.toJSON(generator, document);
    generator.close();

    final String json = writer.toString();

    final JsonParser parser = factory.createParser(json);

    final ODocument resultedDocument = (ODocument) serializer.fromJSON(parser, null);

    final ODatabaseDocumentInternal sessionInternal = (ODatabaseDocumentInternal) session;

    assertDocumentsAreTheSame(document, resultedDocument, sessionInternal);
  }

  private void assertDocumentsAreTheSame(
      ODocument document, ODocument resultedDocument, ODatabaseDocumentInternal sessionInternal) {
    Assert.assertEquals(document.getClassName(), resultedDocument.getClassName());
    Assert.assertEquals(document.getVersion(), resultedDocument.getVersion());
    Assert.assertEquals(document.getIdentity(), resultedDocument.getIdentity());

    Assert.assertTrue(
        ODocumentHelper.hasSameContentOf(
            document, sessionInternal, resultedDocument, sessionInternal, null));
  }
}
