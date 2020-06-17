package com.orientechnologies.orient.core.serialization.serializer.result.binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by luigidellaquila on 09/12/16. */
public class OResultSerializationTest {

  protected OResultSerializerNetwork serializer;

  @Before
  public void before() {
    serializer = new OResultSerializerNetwork();
  }

  @After
  public void after() {}

  @Test
  public void testSimpleSerialization() {
    OResultInternal document = new OResultInternal();

    document.setProperty("name", "name");
    document.setProperty("age", 20);
    document.setProperty("youngAge", (short) 20);
    document.setProperty("oldAge", (long) 20);
    document.setProperty("heigth", 12.5f);
    document.setProperty("bitHeigth", 12.5d);
    document.setProperty("class", (byte) 'C');
    document.setProperty("character", 'C');
    document.setProperty("alive", true);
    document.setProperty("date", new Date());
    document.setProperty("recordId", new ORecordId(10, 10));

    OResultInternal extr = serializeDeserialize(document);

    assertEquals(extr.getPropertyNames(), document.getPropertyNames());
    assertEquals(extr.<String>getProperty("name"), document.getProperty("name"));
    assertEquals(extr.<String>getProperty("age"), document.getProperty("age"));
    assertEquals(extr.<String>getProperty("youngAge"), document.getProperty("youngAge"));
    assertEquals(extr.<String>getProperty("oldAge"), document.getProperty("oldAge"));
    assertEquals(extr.<String>getProperty("heigth"), document.getProperty("heigth"));
    assertEquals(extr.<String>getProperty("bitHeigth"), document.getProperty("bitHeigth"));
    assertEquals(extr.<String>getProperty("class"), document.getProperty("class"));
    // TODO fix char management issue:#2427
    // assertEquals(document.getProperty("character"), extr.getProperty("character"));
    assertEquals(extr.<String>getProperty("alive"), document.getProperty("alive"));
    assertEquals(extr.<String>getProperty("date"), document.getProperty("date"));
    // assertEquals(extr.field("recordId"), document.field("recordId"));

  }

  private OResultInternal serializeDeserialize(OResultInternal document) {
    BytesContainer bytes = new BytesContainer();
    serializer.serialize(document, bytes);
    bytes.offset = 0;
    return serializer.deserialize(bytes);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test
  public void testSimpleLiteralList() {

    OResultInternal document = new OResultInternal();
    List<String> strings = new ArrayList<String>();
    strings.add("a");
    strings.add("b");
    strings.add("c");
    document.setProperty("listStrings", strings);

    List<Short> shorts = new ArrayList<Short>();
    shorts.add((short) 1);
    shorts.add((short) 2);
    shorts.add((short) 3);
    document.setProperty("shorts", shorts);

    List<Long> longs = new ArrayList<Long>();
    longs.add((long) 1);
    longs.add((long) 2);
    longs.add((long) 3);
    document.setProperty("longs", longs);

    List<Integer> ints = new ArrayList<Integer>();
    ints.add(1);
    ints.add(2);
    ints.add(3);
    document.setProperty("integers", ints);

    List<Float> floats = new ArrayList<Float>();
    floats.add(1.1f);
    floats.add(2.2f);
    floats.add(3.3f);
    document.setProperty("floats", floats);

    List<Double> doubles = new ArrayList<Double>();
    doubles.add(1.1);
    doubles.add(2.2);
    doubles.add(3.3);
    document.setProperty("doubles", doubles);

    List<Date> dates = new ArrayList<Date>();
    dates.add(new Date());
    dates.add(new Date());
    dates.add(new Date());
    document.setProperty("dates", dates);

    List<Byte> bytes = new ArrayList<Byte>();
    bytes.add((byte) 0);
    bytes.add((byte) 1);
    bytes.add((byte) 3);
    document.setProperty("bytes", bytes);

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
    document.setProperty("booleans", booleans);

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
    listMixed.add(null);
    document.setProperty("listMixed", listMixed);

    OResult extr = serializeDeserialize(document);

    assertEquals(extr.getPropertyNames(), document.getPropertyNames());
    assertEquals(extr.<String>getProperty("listStrings"), document.getProperty("listStrings"));
    assertEquals(extr.<String>getProperty("integers"), document.getProperty("integers"));
    assertEquals(extr.<String>getProperty("doubles"), document.getProperty("doubles"));
    assertEquals(extr.<String>getProperty("dates"), document.getProperty("dates"));
    assertEquals(extr.<String>getProperty("bytes"), document.getProperty("bytes"));
    assertEquals(extr.<String>getProperty("booleans"), document.getProperty("booleans"));
    assertEquals(extr.<String>getProperty("listMixed"), document.getProperty("listMixed"));
  }

  @Test
  public void testSimpleMapStringLiteral() {
    OResultInternal document = new OResultInternal();

    Map<String, String> mapString = new HashMap<String, String>();
    mapString.put("key", "value");
    mapString.put("key1", "value1");
    document.setProperty("mapString", mapString);

    Map<String, Integer> mapInt = new HashMap<String, Integer>();
    mapInt.put("key", 2);
    mapInt.put("key1", 3);
    document.setProperty("mapInt", mapInt);

    Map<String, Long> mapLong = new HashMap<String, Long>();
    mapLong.put("key", 2L);
    mapLong.put("key1", 3L);
    document.setProperty("mapLong", mapLong);

    Map<String, Short> shortMap = new HashMap<String, Short>();
    shortMap.put("key", (short) 2);
    shortMap.put("key1", (short) 3);
    document.setProperty("shortMap", shortMap);

    Map<String, Date> dateMap = new HashMap<String, Date>();
    dateMap.put("key", new Date());
    dateMap.put("key1", new Date());
    document.setProperty("dateMap", dateMap);

    Map<String, Float> floatMap = new HashMap<String, Float>();
    floatMap.put("key", 10f);
    floatMap.put("key1", 11f);
    document.setProperty("floatMap", floatMap);

    Map<String, Double> doubleMap = new HashMap<String, Double>();
    doubleMap.put("key", 10d);
    doubleMap.put("key1", 11d);
    document.setProperty("doubleMap", doubleMap);

    Map<String, Byte> bytesMap = new HashMap<String, Byte>();
    bytesMap.put("key", (byte) 10);
    bytesMap.put("key1", (byte) 11);
    document.setProperty("bytesMap", bytesMap);

    OResult extr = serializeDeserialize(document);

    assertEquals(extr.getPropertyNames(), document.getPropertyNames());
    assertEquals(extr.<String>getProperty("mapString"), document.getProperty("mapString"));
    assertEquals(extr.<String>getProperty("mapLong"), document.getProperty("mapLong"));
    assertEquals(extr.<String>getProperty("shortMap"), document.getProperty("shortMap"));
    assertEquals(extr.<String>getProperty("dateMap"), document.getProperty("dateMap"));
    assertEquals(extr.<String>getProperty("doubleMap"), document.getProperty("doubleMap"));
    assertEquals(extr.<String>getProperty("bytesMap"), document.getProperty("bytesMap"));
  }

  @Test
  public void testSimpleEmbeddedDoc() {
    OResultInternal document = new OResultInternal();
    OResultInternal embedded = new OResultInternal();
    embedded.setProperty("name", "test");
    embedded.setProperty("surname", "something");
    document.setProperty("embed", embedded);

    OResult extr = serializeDeserialize(document);

    assertEquals(document.getPropertyNames(), extr.getPropertyNames());
    OResult emb = extr.getProperty("embed");
    assertNotNull(emb);
    assertEquals(emb.<String>getProperty("name"), embedded.getProperty("name"));
    assertEquals(emb.<String>getProperty("surname"), embedded.getProperty("surname"));
  }

  @Test
  public void testMapOfEmbeddedDocument() {

    OResultInternal document = new OResultInternal();

    OResultInternal embeddedInMap = new OResultInternal();
    embeddedInMap.setProperty("name", "test");
    embeddedInMap.setProperty("surname", "something");
    Map<String, OResult> map = new HashMap<String, OResult>();
    map.put("embedded", embeddedInMap);
    document.setProperty("map", map);

    OResult extr = serializeDeserialize(document);

    Map<String, OResult> mapS = extr.getProperty("map");
    assertEquals(1, mapS.size());
    OResult emb = mapS.get("embedded");
    assertNotNull(emb);
    assertEquals(emb.<String>getProperty("name"), embeddedInMap.getProperty("name"));
    assertEquals(emb.<String>getProperty("surname"), embeddedInMap.getProperty("surname"));
  }

  @Test
  public void testCollectionOfEmbeddedDocument() {

    OResultInternal document = new OResultInternal();

    OResultInternal embeddedInList = new OResultInternal();
    embeddedInList.setProperty("name", "test");
    embeddedInList.setProperty("surname", "something");

    List<OResult> embeddedList = new ArrayList<OResult>();
    embeddedList.add(embeddedInList);
    document.setProperty("embeddedList", embeddedList);

    OResultInternal embeddedInSet = new OResultInternal();
    embeddedInSet.setProperty("name", "test1");
    embeddedInSet.setProperty("surname", "something2");

    Set<OResult> embeddedSet = new HashSet<>();
    embeddedSet.add(embeddedInSet);
    document.setProperty("embeddedSet", embeddedSet);

    OResult extr = serializeDeserialize(document);

    List<OResult> ser = extr.getProperty("embeddedList");
    assertEquals(1, ser.size());
    OResult inList = ser.get(0);
    assertNotNull(inList);
    assertEquals(inList.<String>getProperty("name"), embeddedInList.getProperty("name"));
    assertEquals(inList.<String>getProperty("surname"), embeddedInList.getProperty("surname"));

    Set<OResult> setEmb = extr.getProperty("embeddedSet");
    assertEquals(1, setEmb.size());
    OResult inSet = setEmb.iterator().next();
    assertNotNull(inSet);
    assertEquals(inSet.<String>getProperty("name"), embeddedInSet.getProperty("name"));
    assertEquals(inSet.<String>getProperty("surname"), embeddedInSet.getProperty("surname"));
  }

  @Test
  public void testMetadataSerialization() {
    OResultInternal document = new OResultInternal();

    document.setProperty("name", "foo");

    document.setMetadata("name", "bar");
    document.setMetadata("age", 20);
    document.setMetadata("youngAge", (short) 20);
    document.setMetadata("oldAge", (long) 20);
    document.setMetadata("heigth", 12.5f);
    document.setMetadata("bitHeigth", 12.5d);
    document.setMetadata("class", (byte) 'C');
    document.setMetadata("alive", true);
    document.setMetadata("date", new Date());

    OResultInternal extr = serializeDeserialize(document);

    assertEquals(extr.getPropertyNames(), document.getPropertyNames());
    assertEquals(extr.<String>getProperty("foo"), document.getProperty("foo"));
    assertEquals("foo", extr.<String>getProperty("name"));

    assertEquals(extr.getMetadataKeys(), document.getMetadataKeys());
    assertEquals("bar", extr.<String>getMetadata("name"));
    assertEquals(extr.<String>getMetadata("name"), document.getMetadata("name"));
    assertEquals(extr.<String>getMetadata("age"), document.getMetadata("age"));
    assertEquals(extr.<String>getMetadata("youngAge"), document.getMetadata("youngAge"));
    assertEquals(extr.<String>getMetadata("oldAge"), document.getMetadata("oldAge"));
    assertEquals(extr.<String>getMetadata("heigth"), document.getMetadata("heigth"));
    assertEquals(extr.<String>getMetadata("bitHeigth"), document.getMetadata("bitHeigth"));
    assertEquals(extr.<String>getMetadata("class"), document.getMetadata("class"));
    assertEquals(extr.<String>getMetadata("alive"), document.getMetadata("alive"));
    assertEquals(extr.<String>getMetadata("date"), document.getMetadata("date"));
  }
}
