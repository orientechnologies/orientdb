package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class ODocumentSchemalessBinarySerializationTest {

  protected ORecordSerializer serializer;

  public static class Custom implements OSerializableStream {
    byte[] bytes = new byte[10];

    @Override
    public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
      bytes = iStream;
      return this;
    }

    @Override
    public byte[] toStream() throws OSerializationException {
      for (int i = 0; i < bytes.length; i++) {
        bytes[i] = (byte) i;
      }
      return bytes;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null && obj instanceof Custom && Arrays.equals(bytes, ((Custom) obj).bytes);
    }
  }

  public static class CustomDocument implements ODocumentSerializable {
    private ODocument document;

    @Override
    public void fromDocument(ODocument document) {
      this.document = document;
    }

    @Override
    public ODocument toDocument() {
      document = new ODocument();
      document.field("test", "some strange content");
      return document;
    }

    @Override
    public boolean equals(Object obj) {
      return obj != null && document.field("test").equals(((CustomDocument) obj).document.field("test"));
    }
  }

  public ODocumentSchemalessBinarySerializationTest() {
    serializer = new ORecordSerializerBinary();
  }

  @Test
  public void testSimpleSerialization() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();

    document.field("name", "name");
    document.field("age", 20);
    document.field("youngAge", (short) 20);
    document.field("oldAge", (long) 20);
    document.field("heigth", 12.5f);
    document.field("bitHeigth", 12.5d);
    document.field("class", (byte) 'C');
    document.field("nullField", (Object) null);
    document.field("character", 'C');
    document.field("alive", true);
    document.field("dateTime", new Date());
    document.field("bigNumber", new BigDecimal("43989872423376487952454365232141525434.32146432321442534"));
    ORidBag bag = new ORidBag();
    bag.add(new ORecordId(1, 1));
    bag.add(new ORecordId(2, 2));
    // document.field("ridBag", bag);
    Calendar c = Calendar.getInstance();
    document.field("date", c.getTime(), OType.DATE);
    Calendar c1 = Calendar.getInstance();
    c1.set(Calendar.MILLISECOND, 0);
    c1.set(Calendar.SECOND, 0);
    c1.set(Calendar.MINUTE, 0);
    c1.set(Calendar.HOUR_OF_DAY, 0);
    document.field("date1", c1.getTime(), OType.DATE);

    byte[] byteValue = new byte[10];
    Arrays.fill(byteValue, (byte) 10);
    document.field("bytes", byteValue);

    document.field("utf8String", new String("A" + "\u00ea" + "\u00f1" + "\u00fc" + "C"));
    document.field("recordId", new ORecordId(10, 10));

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    c.set(Calendar.MILLISECOND, 0);
    c.set(Calendar.SECOND, 0);
    c.set(Calendar.MINUTE, 0);
    c.set(Calendar.HOUR_OF_DAY, 0);

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("name"), document.field("name"));
    assertEquals(extr.field("age"), document.field("age"));
    assertEquals(extr.field("youngAge"), document.field("youngAge"));
    assertEquals(extr.field("oldAge"), document.field("oldAge"));
    assertEquals(extr.field("heigth"), document.field("heigth"));
    assertEquals(extr.field("bitHeigth"), document.field("bitHeigth"));
    assertEquals(extr.field("class"), document.field("class"));
    // TODO fix char management issue:#2427
    // assertEquals(document.field("character"), extr.field("character"));
    assertEquals(extr.field("alive"), document.field("alive"));
    assertEquals(extr.field("dateTime"), document.field("dateTime"));
    assertEquals(extr.field("date"), c.getTime());
    assertEquals(extr.field("date1"), c1.getTime());
    assertEquals(extr.field("bytes"), document.field("bytes"));
    assertEquals(extr.field("utf8String"), document.field("utf8String"));
    assertEquals(extr.field("recordId"), document.field("recordId"));
    assertEquals(extr.field("bigNumber"), document.field("bigNumber"));
    assertNull(extr.field("nullField"));
    // assertEquals(extr.field("ridBag"), document.field("ridBag"));

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSimpleLiteralArray() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    String[] strings = new String[3];
    strings[0] = "a";
    strings[1] = "b";
    strings[2] = "c";
    document.field("listStrings", strings);

    Short[] shorts = new Short[3];
    shorts[0] = (short) 1;
    shorts[1] = (short) 2;
    shorts[2] = (short) 3;
    document.field("shorts", shorts);

    Long[] longs = new Long[3];
    longs[0] = (long) 1;
    longs[1] = (long) 2;
    longs[2] = (long) 3;
    document.field("longs", longs);

    Integer[] ints = new Integer[3];
    ints[0] = 1;
    ints[1] = 2;
    ints[2] = 3;
    document.field("integers", ints);

    Float[] floats = new Float[3];
    floats[0] = 1.1f;
    floats[1] = 2.2f;
    floats[2] = 3.3f;
    document.field("floats", floats);

    Double[] doubles = new Double[3];
    doubles[0] = 1.1d;
    doubles[1] = 2.2d;
    doubles[2] = 3.3d;
    document.field("doubles", doubles);

    Date[] dates = new Date[3];
    dates[0] = new Date();
    dates[1] = new Date();
    dates[2] = new Date();
    document.field("dates", dates);

    Byte[] bytes = new Byte[3];
    bytes[0] = (byte) 0;
    bytes[1] = (byte) 1;
    bytes[2] = (byte) 3;
    document.field("bytes", bytes);

    // TODO: char not currently supported in orient.
    Character[] chars = new Character[3];
    chars[0] = 'A';
    chars[1] = 'B';
    chars[2] = 'C';
    // document.field("chars", chars);

    Boolean[] booleans = new Boolean[3];
    booleans[0] = true;
    booleans[1] = false;
    booleans[2] = false;
    document.field("booleans", booleans);

    Object[] arrayNulls = new Object[3];
    // document.field("arrayNulls", arrayNulls);

    // Object[] listMixed = new ArrayList[9];
    // listMixed[0] = new Boolean(true);
    // listMixed[1] = 1;
    // listMixed[2] = (long) 5;
    // listMixed[3] = (short) 2;
    // listMixed[4] = 4.0f;
    // listMixed[5] = 7.0D;
    // listMixed[6] = "hello";
    // listMixed[7] = new Date();
    // listMixed[8] = (byte) 10;
    // document.field("listMixed", listMixed);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    assertEquals(extr.fields(), document.fields());
    assertEquals(((List) extr.field("listStrings")).toArray(), document.field("listStrings"));
    assertEquals(((List) extr.field("integers")).toArray(), document.field("integers"));
    assertEquals(((List) extr.field("doubles")).toArray(), document.field("doubles"));
    assertEquals(((List) extr.field("dates")).toArray(), document.field("dates"));
    assertEquals(((List) extr.field("bytes")).toArray(), document.field("bytes"));
    assertEquals(((List) extr.field("booleans")).toArray(), document.field("booleans"));
    // assertEquals(((List) extr.field("arrayNulls")).toArray(), document.field("arrayNulls"));
    // assertEquals(extr.field("listMixed"), document.field("listMixed"));
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSimpleLiteralList() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    List<String> strings = new ArrayList<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.field("listStrings", strings);

    List<Short> shorts = new ArrayList<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.field("shorts", shorts);

    List<Long> longs = new ArrayList<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.field("longs", longs);

    List<Integer> ints = new ArrayList<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.field("integers", ints);

    List<Float> floats = new ArrayList<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.field("floats", floats);

    List<Double> doubles = new ArrayList<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.field("doubles", doubles);

    List<Date> dates = new ArrayList<Date>();
    dates.add(new Date());
    dates.add(new Date());
    dates.add(new Date());
    document.field("dates", dates);

    List<Byte> bytes = new ArrayList<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.field("bytes", bytes);

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
    document.field("booleans", booleans);

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
    document.field("listMixed", listMixed);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("listStrings"), document.field("listStrings"));
    assertEquals(extr.field("integers"), document.field("integers"));
    assertEquals(extr.field("doubles"), document.field("doubles"));
    assertEquals(extr.field("dates"), document.field("dates"));
    assertEquals(extr.field("bytes"), document.field("bytes"));
    assertEquals(extr.field("booleans"), document.field("booleans"));
    assertEquals(extr.field("listMixed"), document.field("listMixed"));
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSimpleLiteralSet() throws InterruptedException {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    Set<String> strings = new HashSet<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.field("listStrings", strings);

    Set<Short> shorts = new HashSet<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.field("shorts", shorts);

    Set<Long> longs = new HashSet<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.field("longs", longs);

    Set<Integer> ints = new HashSet<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.field("integers", ints);

    Set<Float> floats = new HashSet<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.field("floats", floats);

    Set<Double> doubles = new HashSet<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.field("doubles", doubles);

    Set<Date> dates = new HashSet<Date>();
    dates.add(new Date());
    Thread.sleep(1);
    dates.add(new Date());
    Thread.sleep(1);
    dates.add(new Date());
    document.field("dates", dates);

    Set<Byte> bytes = new HashSet<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.field("bytes", bytes);

    // TODO: char not currently supported in orient.
    Set<Character> chars = new HashSet<Character>();
    chars.add('A');
    chars.add('B');
    chars.add('C');
    // document.field("chars", chars);

    Set<Boolean> booleans = new HashSet<Boolean>();
    booleans.add(true);
    booleans.add(false);
    booleans.add(false);
    document.field("booleans", booleans);

    Set listMixed = new HashSet();
    listMixed.add(true);
    listMixed.add(1);
    listMixed.add((long) 5);
    listMixed.add((short) 2);
    listMixed.add(4.0f);
    listMixed.add(7.0D);
    listMixed.add("hello");
    listMixed.add(new Date());
    listMixed.add((byte) 10);
    listMixed.add(new ORecordId(10, 20));
    document.field("listMixed", listMixed);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("listStrings"), document.field("listStrings"));
    assertEquals(extr.field("integers"), document.field("integers"));
    assertEquals(extr.field("doubles"), document.field("doubles"));
    assertEquals(extr.field("dates"), document.field("dates"));
    assertEquals(extr.field("bytes"), document.field("bytes"));
    assertEquals(extr.field("booleans"), document.field("booleans"));
    assertEquals(extr.field("listMixed"), document.field("listMixed"));
  }

  @Test
  public void testLinkCollections() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:ODocumentSchemalessBinarySerializationTest").create();
    try {
      ODocument document = new ODocument();
      Set<ORecordId> linkSet = new HashSet<ORecordId>();
      linkSet.add(new ORecordId(10, 20));
      linkSet.add(new ORecordId(10, 21));
      linkSet.add(new ORecordId(10, 22));
      linkSet.add(new ORecordId(11, 22));
      document.field("linkSet", linkSet, OType.LINKSET);

      List<ORecordId> linkList = new ArrayList<ORecordId>();
      linkList.add(new ORecordId(10, 20));
      linkList.add(new ORecordId(10, 21));
      linkList.add(new ORecordId(10, 22));
      linkList.add(new ORecordId(11, 22));
      document.field("linkList", linkList, OType.LINKLIST);
      byte[] res = serializer.toStream(document, false);
      ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

      assertEquals(extr.fields(), document.fields());
      assertEquals(((Set<?>) extr.field("linkSet")).size(), ((Set<?>) document.field("linkSet")).size());
      assertTrue(((Set<?>) extr.field("linkSet")).containsAll((Set<?>) document.field("linkSet")));
      assertEquals(extr.field("linkList"), document.field("linkList"));
    } finally {
      db.drop();
    }

  }

  @Test
  public void testSimpleEmbeddedDoc() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    ODocument embedded = new ODocument();
    embedded.field("name", "test");
    embedded.field("surname", "something");
    document.field("embed", embedded, OType.EMBEDDED);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(document.fields(), extr.fields());
    ODocument emb = extr.field("embed");
    assertNotNull(emb);
    assertEquals(emb.field("name"), embedded.field("name"));
    assertEquals(emb.field("surname"), embedded.field("surname"));
  }

  @Test
  public void testSimpleMapStringLiteral() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();

    Map<String, String> mapString = new HashMap<String, String>();
    mapString.put("key", "value");
    mapString.put("key1", "value1");
    document.field("mapString", mapString);

    Map<String, Integer> mapInt = new HashMap<String, Integer>();
    mapInt.put("key", 2);
    mapInt.put("key1", 3);
    document.field("mapInt", mapInt);

    Map<String, Long> mapLong = new HashMap<String, Long>();
    mapLong.put("key", 2L);
    mapLong.put("key1", 3L);
    document.field("mapLong", mapLong);

    Map<String, Short> shortMap = new HashMap<String, Short>();
    shortMap.put("key", (short) 2);
    shortMap.put("key1", (short) 3);
    document.field("shortMap", shortMap);

    Map<String, Date> dateMap = new HashMap<String, Date>();
    dateMap.put("key", new Date());
    dateMap.put("key1", new Date());
    document.field("dateMap", dateMap);

    Map<String, Float> floatMap = new HashMap<String, Float>();
    floatMap.put("key", 10f);
    floatMap.put("key1", 11f);
    document.field("floatMap", floatMap);

    Map<String, Double> doubleMap = new HashMap<String, Double>();
    doubleMap.put("key", 10d);
    doubleMap.put("key1", 11d);
    document.field("doubleMap", doubleMap);

    Map<String, Byte> bytesMap = new HashMap<String, Byte>();
    bytesMap.put("key", (byte) 10);
    bytesMap.put("key1", (byte) 11);
    document.field("bytesMap", bytesMap);

    Map<String, String> mapWithNulls = new HashMap<String, String>();
    mapWithNulls.put("key", "dddd");
    mapWithNulls.put("key1", null);
    document.field("bytesMap", mapWithNulls);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("mapString"), document.field("mapString"));
    assertEquals(extr.field("mapLong"), document.field("mapLong"));
    assertEquals(extr.field("shortMap"), document.field("shortMap"));
    assertEquals(extr.field("dateMap"), document.field("dateMap"));
    assertEquals(extr.field("doubleMap"), document.field("doubleMap"));
    assertEquals(extr.field("bytesMap"), document.field("bytesMap"));
  }

  @Test
  public void testlistOfList() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    List<List<String>> list = new ArrayList<List<String>>();
    List<String> ls = new ArrayList<String>();
    ls.add("test1");
    ls.add("test2");
    list.add(ls);
    document.field("complexList", list);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("complexList"), document.field("complexList"));

  }

  @Test
  public void testArrayOfArray() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    String[][] array = new String[1][];
    String[] ls = new String[2];
    ls[0] = "test1";
    ls[1] = "test2";
    array[0] = ls;
    document.field("complexArray", array);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.fields(), document.fields());
    List<List<String>> savedValue = extr.field("complexArray");
    assertEquals(savedValue.size(), array.length);
    assertEquals(savedValue.get(0).size(), array[0].length);
    assertEquals(savedValue.get(0).get(0), array[0][0]);
    assertEquals(savedValue.get(0).get(1), array[0][1]);

  }

  @Test
  public void testEmbeddedListOfEmbeddedMap() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();
    List<Map<String, String>> coll = new ArrayList<Map<String, String>>();
    Map<String, String> map = new HashMap<String, String>();
    map.put("first", "something");
    map.put("second", "somethingElse");
    Map<String, String> map2 = new HashMap<String, String>();
    map2.put("first", "something");
    map2.put("second", "somethingElse");
    coll.add(map);
    coll.add(map2);
    document.field("list", coll);
    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("list"), document.field("list"));
  }

  @Test
  public void testMapOfEmbeddedDocument() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    ODocument embeddedInMap = new ODocument();
    embeddedInMap.field("name", "test");
    embeddedInMap.field("surname", "something");
    Map<String, ODocument> map = new HashMap<String, ODocument>();
    map.put("embedded", embeddedInMap);
    document.field("map", map, OType.EMBEDDEDMAP);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    Map<String, ODocument> mapS = extr.field("map");
    assertEquals(1, mapS.size());
    ODocument emb = mapS.get("embedded");
    assertNotNull(emb);
    assertEquals(emb.field("name"), embeddedInMap.field("name"));
    assertEquals(emb.field("surname"), embeddedInMap.field("surname"));
  }

  @Test
  public void testMapOfLink() {
    // needs a database because of the lazy loading
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:ODocumentSchemalessBinarySerializationTest").create();
    try {
      ODocument document = new ODocument();

      Map<String, OIdentifiable> map = new HashMap<String, OIdentifiable>();
      map.put("link", new ORecordId(0, 0));
      document.field("map", map, OType.LINKMAP);

      byte[] res = serializer.toStream(document, false);
      ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
      assertEquals(extr.fields(), document.fields());
      assertEquals(extr.field("map"), document.field("map"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testDocumentWithClassName() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:ODocumentSchemalessBinarySerializationTest").create();
    try {
      ODocument document = new ODocument("TestClass");
      document.field("test", "test");
      byte[] res = serializer.toStream(document, false);
      ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
      assertEquals(extr.getClassName(), document.getClassName());
      assertEquals(extr.fields(), document.fields());
      assertEquals(extr.field("test"), document.field("test"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testDocumentWithCostum() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    document.field("test", "test");
    document.field("custom", new Custom());
    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.getClassName(), document.getClassName());
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("test"), document.field("test"));
    assertEquals(extr.field("custom"), document.field("custom"));
  }

  @Test
  public void testDocumentWithCostumDocument() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
    ODocument document = new ODocument();
    document.field("test", "test");
    document.field("custom", new CustomDocument());
    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    assertEquals(extr.getClassName(), document.getClassName());
    assertEquals(extr.fields(), document.fields());
    assertEquals(extr.field("test"), document.field("test"));
    assertEquals(extr.field("custom"), document.field("custom"));
  }

  private class WrongData {

  }

  @Test(expectedExceptions = OSerializationException.class)
  private void testSetOfWrongData() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    Set<Object> embeddedSet = new HashSet<Object>();
    embeddedSet.add(new WrongData());
    document.field("embeddedSet", embeddedSet, OType.EMBEDDEDSET);

    serializer.toStream(document, false);
  }

  @Test(expectedExceptions = OSerializationException.class)
  private void testListOfWrongData() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    List<Object> embeddedList = new ArrayList<Object>();
    embeddedList.add(new WrongData());
    document.field("embeddedList", embeddedList, OType.EMBEDDEDLIST);

    serializer.toStream(document, false);
  }

  @Test(expectedExceptions = OSerializationException.class)
  private void testMapOfWrongData() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    Map<String, Object> embeddedMap = new HashMap<String, Object>();
    embeddedMap.put("name", new WrongData());
    document.field("embeddedMap", embeddedMap, OType.EMBEDDEDMAP);

    serializer.toStream(document, false);
  }

  @Test(expectedExceptions = ClassCastException.class)
  private void testLinkSetOfWrongData() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    Set<Object> linkSet = new HashSet<Object>();
    linkSet.add(new WrongData());
    document.field("linkSet", linkSet, OType.LINKSET);

    serializer.toStream(document, false);
  }

  @Test(expectedExceptions = ClassCastException.class)
  private void testLinkListOfWrongData() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    List<Object> linkList = new ArrayList<Object>();
    linkList.add(new WrongData());
    document.field("linkList", linkList, OType.LINKLIST);

    serializer.toStream(document, false);
  }

  @Test(expectedExceptions = ClassCastException.class)
  private void testLinkMapOfWrongData() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    Map<String, Object> linkMap = new HashMap<String, Object>();
    linkMap.put("name", new WrongData());
    document.field("linkMap", linkMap, OType.LINKMAP);

    serializer.toStream(document, false);
  }

  @Test(expectedExceptions = OSerializationException.class)
  private void testFieldWrongData() {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    document.field("wrongData", new WrongData());

    serializer.toStream(document, false);
  }

  @Test
  private void testCollectionOfEmbeddedDocument() {

    ODatabaseRecordThreadLocal.INSTANCE.remove();

    ODocument document = new ODocument();

    ODocument embeddedInList = new ODocument();
    embeddedInList.field("name", "test");
    embeddedInList.field("surname", "something");

    ODocument embeddedInList2 = new ODocument();
    embeddedInList2.field("name", "test1");
    embeddedInList2.field("surname", "something2");

    List<ODocument> embeddedList = new ArrayList<ODocument>();
    embeddedList.add(embeddedInList);
    embeddedList.add(embeddedInList2);
    embeddedList.add(new ODocument());
    document.field("embeddedList", embeddedList, OType.EMBEDDEDLIST);

    ODocument embeddedInSet = new ODocument();
    embeddedInSet.field("name", "test2");
    embeddedInSet.field("surname", "something3");

    ODocument embeddedInSet2 = new ODocument();
    embeddedInSet2.field("name", "test5");
    embeddedInSet2.field("surname", "something6");

    Set<ODocument> embeddedSet = new HashSet<ODocument>();
    embeddedSet.add(embeddedInSet);
    embeddedSet.add(embeddedInSet2);
    embeddedSet.add(new ODocument());
    document.field("embeddedSet", embeddedSet, OType.EMBEDDEDSET);

    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    List<ODocument> ser = extr.field("embeddedList");
    assertEquals(ser.size(), 3);
    assertNotNull(ser.get(0));
    assertNotNull(ser.get(1));
    assertNotNull(ser.get(2));
    ODocument inList = ser.get(0);
    assertNotNull(inList);
    assertEquals(inList.field("name"), embeddedInList.field("name"));
    assertEquals(inList.field("surname"), embeddedInList.field("surname"));

    Set<ODocument> setEmb = extr.field("embeddedSet");
    assertEquals(setEmb.size(), 3);
    boolean ok = false;
    for (ODocument inSet : setEmb) {
      assertNotNull(inSet);
      if (embeddedInSet.field("name").equals(inSet.field("name")) && embeddedInSet.field("surname").equals(inSet.field("surname")))
        ok = true;
    }
    assertTrue(ok, "not found record in the set after serilize");
  }

  @Test
  public void testSerializableValue() {
    ODocument document = new ODocument();
    SimpleSerializableClass ser = new SimpleSerializableClass();
    ser.name = "testName";
    document.field("seri", ser);
    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    assertNotNull(extr.field("seri"));
    assertEquals(extr.fieldType("seri"), OType.CUSTOM);
    SimpleSerializableClass newser = extr.field("seri");
    assertEquals(newser.name, ser.name);

  }

  @Test
  public void testFieldNames() {
    ODocument document = new ODocument();
    document.fields("a", 1, "b", 2, "c", 3);
    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});

    final String[] fields = extr.fieldNames();

    assertNotNull(fields);
    assertEquals(fields.length, 3);
    assertEquals(fields[0], "a");
    assertEquals(fields[1], "b");
    assertEquals(fields[2], "c");
  }
}
