package com.orientechnologies.orient.core.serialization.serializer.record.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
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
    testSerializationDeserialization(document -> {});
  }

  @Test
  public void serializePrimitiveFields() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("serializePrimitiveFields seed : " + seed);
    final Random random = new Random(seed);

    testSerializationDeserialization(document -> generatePrimitiveTypes(document, random));
  }

  @Test
  public void serializeLinkedCollections() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("serializeLinkedCollections seed : " + seed);
    final Random random = new Random(seed);

    testSerializationDeserialization(document -> generateLinkedCollections(document, random));
  }

  @Test
  public void serializeEmbeddedCollections() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("serializeEmbeddedCollections seed : " + seed);
    final Random random = new Random(seed);

    testSerializationDeserialization(document -> generateEmbeddedCollections(document, random, 1));
  }

  private void generateEmbeddedCollections(
      final ODocument document, final Random random, final int depth) {
    final List<ODocument> embeddedList = new ArrayList<>();
    final int listSize = random.nextInt(12) + 6;

    for (int i = 0; i < listSize; i++) {
      final ODocument doc = new ODocument();
      generatePrimitiveTypes(doc, random);
      generateLinkedCollections(doc, random);

      if (depth < 3 && random.nextBoolean()) {
        generateEmbeddedCollections(doc, random, depth + 1);
      }

      embeddedList.add(doc);
    }

    final Set<ODocument> embeddedSet = new HashSet<>();
    final int setSize = random.nextInt(12) + 6;

    for (int i = 0; i < setSize; i++) {
      final ODocument doc = new ODocument();
      generatePrimitiveTypes(doc, random);
      generateLinkedCollections(doc, random);

      if (depth < 3 && random.nextBoolean()) {
        generateEmbeddedCollections(doc, random, depth + 1);
      }

      embeddedSet.add(doc);
    }

    final Map<String, ODocument> embeddedMap = new HashMap<>();
    final int mapSize = random.nextInt(12) + 6;

    for (int i = 0; i < mapSize; i++) {
      final ODocument doc = new ODocument();
      generatePrimitiveTypes(doc, random);
      generateLinkedCollections(doc, random);

      if (depth < 3 && random.nextBoolean()) {
        generateEmbeddedCollections(doc, random, depth + 1);
      }

      embeddedMap.put("key" + i, doc);
    }

    document.field("embeddedList", embeddedList, OType.EMBEDDEDLIST);
    document.field("embeddedSet", embeddedSet, OType.EMBEDDEDSET);
    document.field("embeddedMap", embeddedMap, OType.EMBEDDEDMAP);
  }

  private void generateLinkedCollections(final ODocument document, final Random random) {
    final List<ORID> linkedList = new ArrayList<>();

    linkedList.add(randomRecordId(random));
    linkedList.add(randomRecordId(random));
    linkedList.add(randomRecordId(random));

    final Map<String, ORID> linkedMap = new HashMap<>();

    linkedMap.put("key1", randomRecordId(random));
    linkedMap.put("key2", randomRecordId(random));
    linkedMap.put("key3", randomRecordId(random));
    linkedMap.put("key4", randomRecordId(random));

    final Set<ORID> linkedSet = new HashSet<>();
    linkedSet.add(randomRecordId(random));
    linkedSet.add(randomRecordId(random));
    linkedSet.add(randomRecordId(random));

    document.field("linkedMap", linkedMap, OType.LINKMAP);
    document.field("linkedList", linkedList, OType.LINKLIST);
    document.field("linkedSet", linkedSet, OType.LINKSET);

    final ORidBag ridBag = new ORidBag();
    document.field("ridBag", ridBag, OType.LINKBAG);

    ridBag.add(randomRecordId(random));
    ridBag.add(randomRecordId(random));
    ridBag.add(randomRecordId(random));
  }

  private void generatePrimitiveTypes(ODocument document, final Random random) {
    document.field("sVal", (short) random.nextInt(1 << 16), OType.SHORT);
    document.field("sValNull", null, OType.SHORT);
    document.field("sValNoType", (short) random.nextInt(1 << 16));

    document.field("iVal", random.nextInt(), OType.INTEGER);
    document.field("iValNull", null, OType.INTEGER);
    document.field("iValNoType", random.nextInt());

    document.field("lVal", random.nextLong(), OType.LONG);
    document.field("lValNull", null, OType.LONG);
    document.field("lValNoType", random.nextLong());

    document.field("strVal", "val" + random.nextInt(), OType.STRING);
    document.field("strValNull", null, OType.STRING);
    document.field("strValNoType", "val" + random.nextInt());

    document.field("fVal", random.nextFloat(), OType.FLOAT);
    document.field("fValNull", null, OType.FLOAT);
    document.field("fValNoType", random.nextFloat());

    document.field("dVal", random.nextDouble(), OType.DOUBLE);
    document.field("dValNull", null, OType.DOUBLE);
    document.field("dValNoType", random.nextDouble());

    document.field("bVal", (byte) random.nextInt(1 << 8), OType.BYTE);
    document.field("bValNull", null, OType.BYTE);
    document.field("bValNoType", (byte) random.nextInt(1 << 8));

    document.field("boolVal", random.nextBoolean(), OType.BOOLEAN);
    document.field("boolValNull", null, OType.BOOLEAN);
    document.field("boolValNoType", random.nextBoolean());

    document.field("linkVal", randomRecordId(random), OType.LINK);
    document.field("linkValNull", null, OType.LINK);
    document.field("linkValNoType", randomRecordId(random));

    final int arrayLen = random.nextInt(12);

    byte[] array = new byte[arrayLen];
    random.nextBytes(array);

    document.field("binaryVal", array, OType.BINARY);
    document.field("binaryValNull", null, OType.BINARY);

    array = new byte[arrayLen];
    random.nextBytes(array);

    document.field("binaryValNoType", array);

    document.field("decVal", new BigDecimal("13241235467.960464"), OType.DECIMAL);
    document.field("decValNull", null, OType.DECIMAL);
    document.field("decValNoType", new BigDecimal("13241235467.960464"));

    final Calendar dtCalendar = Calendar.getInstance();
    dtCalendar.set(
        random.nextInt(100) + 200,
        Calendar.OCTOBER,
        random.nextInt(28),
        random.nextInt(24),
        random.nextInt(60),
        random.nextInt(60));

    document.field("dtVal", dtCalendar.getTime(), OType.DATETIME);
    document.field("dtValNull", null, OType.DATETIME);
    document.field("dtValNoType", dtCalendar.getTime());

    dtCalendar.set(random.nextInt(100) + 200, Calendar.NOVEMBER, random.nextInt(28));

    document.field("dateVal", dtCalendar.getTime(), OType.DATE);
    document.field("dateValNull", null, OType.DATE);
    document.field("dateValNoType", dtCalendar.getTime());

    document.setProperty("nullValue", null);
  }

  private static ORID randomRecordId(final Random random) {
    final short clusterId = (short) random.nextInt(1 << 15);
    final int position = random.nextInt(Integer.MAX_VALUE);

    return new ORecordId(clusterId, position);
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

    final JsonToken token = parser.nextToken();
    Assert.assertEquals(JsonToken.START_OBJECT, token);

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
