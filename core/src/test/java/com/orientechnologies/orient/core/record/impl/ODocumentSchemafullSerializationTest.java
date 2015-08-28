package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

public class ODocumentSchemafullSerializationTest {

  private static final String       CITY           = "city";
  private static final String       NUMBER         = "number";
  private static final String       INT_FIELD      = NUMBER;
  private static final String       NAME           = "name";
  private static final String       MAP_BYTES      = "bytesMap";
  private static final String       MAP_DOUBLE     = "doubleMap";
  private static final String       MAP_FLOAT      = "floatMap";
  private static final String       MAP_DATE       = "dateMap";
  private static final String       MAP_SHORT      = "shortMap";
  private static final String       MAP_LONG       = "mapLong";
  private static final String       MAP_INT        = "mapInt";
  private static final String       MAP_STRING     = "mapString";
  private static final String       LIST_MIXED     = "listMixed";
  private static final String       LIST_BOOLEANS  = "booleans";
  private static final String       LIST_BYTES     = "bytes";
  private static final String       LIST_DATES     = "dates";
  private static final String       LIST_DOUBLES   = "doubles";
  private static final String       LIST_FLOATS    = "floats";
  private static final String       LIST_INTEGERS  = "integers";
  private static final String       LIST_LONGS     = "longs";
  private static final String       LIST_SHORTS    = "shorts";
  private static final String       LIST_STRINGS   = "listStrings";
  private static final String       SHORT_FIELD    = "shortNumber";
  private static final String       LONG_FIELD     = "longNumber";
  private static final String       STRING_FIELD   = "stringField";
  private static final String       FLOAT_NUMBER   = "floatNumber";
  private static final String       DOUBLE_NUMBER  = "doubleNumber";
  private static final String       BYTE_FIELD     = "byteField";
  private static final String       BOOLEAN_FIELD  = "booleanField";
  private static final String       DATE_FIELD     = "dateField";
  private static final String       RECORDID_FIELD = "recordField";
  private static final String       EMBEDDED_FIELD = "embeddedField";
  private static final String       ANY_FIELD      = "anyField";
  private ODatabaseDocumentInternal databaseDocument;
  private OClass                    simple;
  private ORecordSerializer         serializer;
  private OClass                    embSimp;
  private OClass                    address;
  private OClass                    embMapSimple;

  public ODocumentSchemafullSerializationTest(ORecordSerializer serializer) {
    this.serializer = serializer;
  }

  public ODocumentSchemafullSerializationTest() {
    this(new ORecordSerializerSchemaAware2CSV());
  }

  @BeforeMethod
  public void before() {
    ODatabaseDocumentTx.setDefaultSerializer(serializer);
    databaseDocument = new ODatabaseDocumentTx("memory:" + ODocumentSchemafullSerializationTest.class.getSimpleName()).create();
    // databaseDocument.getMetadata().
    OSchema schema = databaseDocument.getMetadata().getSchema();
    address = schema.createClass("Address");
    address.createProperty(NAME, OType.STRING);
    address.createProperty(NUMBER, OType.INTEGER);
    address.createProperty(CITY, OType.STRING);

    simple = schema.createClass("Simple");
    simple.createProperty(STRING_FIELD, OType.STRING);
    simple.createProperty(INT_FIELD, OType.INTEGER);
    simple.createProperty(SHORT_FIELD, OType.SHORT);
    simple.createProperty(LONG_FIELD, OType.LONG);
    simple.createProperty(FLOAT_NUMBER, OType.FLOAT);
    simple.createProperty(DOUBLE_NUMBER, OType.DOUBLE);
    simple.createProperty(BYTE_FIELD, OType.BYTE);
    simple.createProperty(BOOLEAN_FIELD, OType.BOOLEAN);
    simple.createProperty(DATE_FIELD, OType.DATETIME);
    simple.createProperty(RECORDID_FIELD, OType.LINK);
    simple.createProperty(EMBEDDED_FIELD, OType.EMBEDDED, address);
    simple.createProperty(ANY_FIELD, OType.ANY);

    embSimp = schema.createClass("EmbeddedCollectionSimple");
    embSimp.createProperty(LIST_BOOLEANS, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_BYTES, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_DATES, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_DOUBLES, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_FLOATS, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_INTEGERS, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_LONGS, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_SHORTS, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_STRINGS, OType.EMBEDDEDLIST);
    embSimp.createProperty(LIST_MIXED, OType.EMBEDDEDLIST);

    embMapSimple = schema.createClass("EmbeddedMapSimple");
    embMapSimple.createProperty(MAP_BYTES, OType.EMBEDDEDMAP);
    embMapSimple.createProperty(MAP_DATE, OType.EMBEDDEDMAP);
    embMapSimple.createProperty(MAP_DOUBLE, OType.EMBEDDEDMAP);
    embMapSimple.createProperty(MAP_FLOAT, OType.EMBEDDEDMAP);
    embMapSimple.createProperty(MAP_INT, OType.EMBEDDEDMAP);
    embMapSimple.createProperty(MAP_LONG, OType.EMBEDDEDMAP);
    embMapSimple.createProperty(MAP_SHORT, OType.EMBEDDEDMAP);
    embMapSimple.createProperty(MAP_STRING, OType.EMBEDDEDMAP);

    OClass clazzEmbComp = schema.createClass("EmbeddedComplex");
    clazzEmbComp.createProperty("addresses", OType.EMBEDDEDLIST, address);
    clazzEmbComp.createProperty("uniqueAddresses", OType.EMBEDDEDSET, address);
    clazzEmbComp.createProperty("addressByStreet", OType.EMBEDDEDMAP, address);
  }

  @AfterMethod
  public void after() {
    databaseDocument.drop();
    ODatabaseDocumentTx.setDefaultSerializer(ORecordSerializerFactory.instance().getFormat(
        OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString()));
  }

  @Test
  public void testSimpleSerialization() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    ODocument document = new ODocument(simple);

    document.field(STRING_FIELD, NAME);
    document.field(INT_FIELD, 20);
    document.field(SHORT_FIELD, (short) 20);
    document.field(LONG_FIELD, (long) 20);
    document.field(FLOAT_NUMBER, 12.5f);
    document.field(DOUBLE_NUMBER, 12.5d);
    document.field(BYTE_FIELD, (byte) 'C');
    document.field(BOOLEAN_FIELD, true);
    document.field(DATE_FIELD, new Date());
    document.field(RECORDID_FIELD, new ORecordId(10, 0));

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field(STRING_FIELD), document.field(STRING_FIELD));
    assertEquals(extr.field(INT_FIELD), document.field(INT_FIELD));
    assertEquals(extr.field(SHORT_FIELD), document.field(SHORT_FIELD));
    assertEquals(extr.field(LONG_FIELD), document.field(LONG_FIELD));
    assertEquals(extr.field(FLOAT_NUMBER), document.field(FLOAT_NUMBER));
    assertEquals(extr.field(DOUBLE_NUMBER), document.field(DOUBLE_NUMBER));
    assertEquals(extr.field(BYTE_FIELD), document.field(BYTE_FIELD));
    assertEquals(extr.field(BOOLEAN_FIELD), document.field(BOOLEAN_FIELD));
    assertEquals(extr.field(DATE_FIELD), document.field(DATE_FIELD));
    assertEquals(extr.field(RECORDID_FIELD), document.field(RECORDID_FIELD));
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSimpleLiteralList() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    ODocument document = new ODocument(embSimp);
    List<String> strings = new ArrayList<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.field(LIST_STRINGS, strings);

    List<Short> shorts = new ArrayList<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.field(LIST_SHORTS, shorts);

    List<Long> longs = new ArrayList<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.field(LIST_LONGS, longs);

    List<Integer> ints = new ArrayList<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.field(LIST_INTEGERS, ints);

    List<Float> floats = new ArrayList<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.field(LIST_FLOATS, floats);

    List<Double> doubles = new ArrayList<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.field(LIST_DOUBLES, doubles);

    List<Date> dates = new ArrayList<Date>();
    dates.add(new Date());
    dates.add(new Date());
    dates.add(new Date());
    document.field(LIST_DATES, dates);

    List<Byte> bytes = new ArrayList<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.field(LIST_BYTES, bytes);

    // TODO: char not currently supported in orient.
    List<Character> chars = new ArrayList<Character>();
    chars.add('A');
    chars.add('B');
    chars.add('C');
    // document.field("chars", chars);

    List<Boolean> booleans = new ArrayList<Boolean>();
    booleans.add(true);
    booleans.add(false);
    booleans.add(false);
    document.field(LIST_BOOLEANS, booleans);

    List listMixed = new ArrayList();
    listMixed.add(true);
    listMixed.add(1);
    listMixed.add((long) 5);
    listMixed.add((short) 2);
    listMixed.add(4.0f);
    listMixed.add(7.0D);
    listMixed.add("hello");
    listMixed.add(new Date());
    listMixed.add((byte) 10);
    document.field(LIST_MIXED, listMixed);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field(LIST_STRINGS), document.field(LIST_STRINGS));
    assertEquals(extr.field(LIST_INTEGERS), document.field(LIST_INTEGERS));
    assertEquals(extr.field(LIST_DOUBLES), document.field(LIST_DOUBLES));
    assertEquals(extr.field(LIST_DATES), document.field(LIST_DATES));
    assertEquals(extr.field(LIST_BYTES), document.field(LIST_BYTES));
    assertEquals(extr.field(LIST_BOOLEANS), document.field(LIST_BOOLEANS));
    assertEquals(extr.field(LIST_MIXED), document.field(LIST_MIXED));
  }

  @Test
  public void testSimpleMapStringLiteral() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    ODocument document = new ODocument(embMapSimple);

    Map<String, String> mapString = new HashMap<String, String>();
    mapString.put("key", "value");
    mapString.put("key1", "value1");
    document.field(MAP_STRING, mapString);

    Map<String, Integer> mapInt = new HashMap<String, Integer>();
    mapInt.put("key", 2);
    mapInt.put("key1", 3);
    document.field(MAP_INT, mapInt);

    Map<String, Long> mapLong = new HashMap<String, Long>();
    mapLong.put("key", 2L);
    mapLong.put("key1", 3L);
    document.field(MAP_LONG, mapLong);

    Map<String, Short> shortMap = new HashMap<String, Short>();
    shortMap.put("key", (short) 2);
    shortMap.put("key1", (short) 3);
    document.field(MAP_SHORT, shortMap);

    Map<String, Date> dateMap = new HashMap<String, Date>();
    dateMap.put("key", new Date());
    dateMap.put("key1", new Date());
    document.field(MAP_DATE, dateMap);

    Map<String, Float> floatMap = new HashMap<String, Float>();
    floatMap.put("key", 10f);
    floatMap.put("key1", 11f);
    document.field(MAP_FLOAT, floatMap);

    Map<String, Double> doubleMap = new HashMap<String, Double>();
    doubleMap.put("key", 10d);
    doubleMap.put("key1", 11d);
    document.field(MAP_DOUBLE, doubleMap);

    Map<String, Byte> bytesMap = new HashMap<String, Byte>();
    bytesMap.put("key", (byte) 10);
    bytesMap.put("key1", (byte) 11);
    document.field(MAP_BYTES, bytesMap);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field(MAP_STRING), document.field(MAP_STRING));
    assertEquals(extr.field(MAP_LONG), document.field(MAP_LONG));
    assertEquals(extr.field(MAP_SHORT), document.field(MAP_SHORT));
    assertEquals(extr.field(MAP_DATE), document.field(MAP_DATE));
    assertEquals(extr.field(MAP_DOUBLE), document.field(MAP_DOUBLE));
    assertEquals(extr.field(MAP_BYTES), document.field(MAP_BYTES));
  }

  @Test
  public void testSimpleEmbeddedDoc() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    ODocument document = new ODocument(simple);
    ODocument embedded = new ODocument(address);
    embedded.field(NAME, "test");
    embedded.field(NUMBER, 1);
    embedded.field(CITY, "aaa");
    document.field(EMBEDDED_FIELD, embedded);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(document.fields(), extr.fields());
    ODocument emb = extr.field(EMBEDDED_FIELD);
    assertNotNull(emb);
    assertEquals(emb.field(NAME), embedded.field(NAME));
    assertEquals(emb.field(NUMBER), embedded.field(NUMBER));
    assertEquals(emb.field(CITY), embedded.field(CITY));
  }

  @Test
  public void testUpdateBooleanWithPropertyTypeAny() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    ODocument document = new ODocument(simple);
    document.field(ANY_FIELD, false);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(document.fields(), extr.fields());
    assertEquals(extr.field(ANY_FIELD), false);

    extr.field(ANY_FIELD, false);

    res = serializer.toStream(extr, false);
    ODocument extr2 = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.fields(), extr2.fields());
    assertEquals(extr2.field(ANY_FIELD), false);

  }

  @Test
  public void simpleTypeKeepingTest() {
    ODatabaseRecordThreadLocal.INSTANCE.set(databaseDocument);
    ODocument document = new ODocument();
    document.field("name", "test");

    byte[] res = serializer.toStream(document, false);
    ODocument extr = new ODocument().fromStream(res);
    assertEquals(OType.STRING, extr.fieldType("name"));

  }

}
