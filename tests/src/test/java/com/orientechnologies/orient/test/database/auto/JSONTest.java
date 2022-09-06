/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
@Test
public class JSONTest extends DocumentDBBaseTest {
  public static final String FORMAT_WITHOUT_TYPES =
      "rid,version,class,type,attribSameRow,alwaysFetchEmbedded,fetchPlan:*:0";

  @Parameters(value = "url")
  public JSONTest(@Optional final String url) {
    super(url);
  }

  @Test
  public void testAlmostLink() {
    final ODocument doc = new ODocument();
    doc.fromJSON("{'title': '#330: Dollar Coins Are Done'}");
  }

  @Test
  public void testNullList() throws Exception {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [\"string\", null]}");

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), "string");
    Assert.assertNull(list.get(1));
  }

  @Test
  public void testBooleanList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [true, false]}");

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), true);
    Assert.assertEquals(list.get(1), false);
  }

  @Test
  public void testNumericIntegerList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [17,42]}");

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 17);
    Assert.assertEquals(list.get(1), 42);
  }

  @Test
  public void testNumericLongList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [100000000000,100000000001]}");

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 100000000000l);
    Assert.assertEquals(list.get(1), 100000000001l);
  }

  @Test
  public void testNumericFloatList() {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [17.3,42.7]}");

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 17.3);
    Assert.assertEquals(list.get(1), 42.7);
  }

  @Test
  public void testNullity() {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        "{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\","
            + "\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith Ave\","
            + "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"},"
            + "\"dob\":\"2011-11-17 03:17:04\"}");
    final String json = doc.toJSON();
    final ODocument loadedDoc = new ODocument().fromJSON(json);
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
  }

  @Test
  public void testNanNoTypes() {
    ODocument doc = new ODocument();
    String input =
        "{\"@type\":\"d\",\"@version\":0,\"nan\":null,\"p_infinity\":null,\"n_infinity\":null}";
    doc.field("nan", Double.NaN);
    doc.field("p_infinity", Double.POSITIVE_INFINITY);
    doc.field("n_infinity", Double.NEGATIVE_INFINITY);
    String json = doc.toJSON(FORMAT_WITHOUT_TYPES);
    Assert.assertEquals(json, input);

    doc = new ODocument();
    input = "{\"@type\":\"d\",\"@version\":0,\"nan\":null,\"p_infinity\":null,\"n_infinity\":null}";
    doc.field("nan", Float.NaN);
    doc.field("p_infinity", Float.POSITIVE_INFINITY);
    doc.field("n_infinity", Float.NEGATIVE_INFINITY);
    json = doc.toJSON(FORMAT_WITHOUT_TYPES);
    Assert.assertEquals(json, input);
  }

  @Test
  public void testEmbeddedList() {
    final ODocument doc = new ODocument();
    final List<ODocument> list = new ArrayList<ODocument>();
    doc.field("embeddedList", list, OType.EMBEDDEDLIST);
    list.add(new ODocument().field("name", "Luca"));
    list.add(new ODocument().field("name", "Marcus"));

    final String json = doc.toJSON();
    final ODocument loadedDoc = new ODocument().fromJSON(json);
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
    Assert.assertTrue(loadedDoc.containsField("embeddedList"));
    Assert.assertTrue(loadedDoc.field("embeddedList") instanceof List<?>);
    Assert.assertTrue(
        ((List<ODocument>) loadedDoc.field("embeddedList")).get(0) instanceof ODocument);

    ODocument newDoc = ((List<ODocument>) loadedDoc.field("embeddedList")).get(0);
    Assert.assertEquals(newDoc.field("name"), "Luca");
    newDoc = ((List<ODocument>) loadedDoc.field("embeddedList")).get(1);
    Assert.assertEquals(newDoc.field("name"), "Marcus");
  }

  @Test
  public void testEmbeddedMap() {
    final ODocument doc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    doc.field("map", map);
    map.put("Luca", new ODocument().field("name", "Luca"));
    map.put("Marcus", new ODocument().field("name", "Marcus"));
    map.put("Cesare", new ODocument().field("name", "Cesare"));

    final String json = doc.toJSON();
    final ODocument loadedDoc = new ODocument().fromJSON(json);

    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("map"));
    Assert.assertTrue(loadedDoc.field("map") instanceof Map<?, ?>);
    Assert.assertTrue(
        ((Map<String, ODocument>) loadedDoc.field("map")).values().iterator().next()
            instanceof ODocument);

    ODocument newDoc = ((Map<String, ODocument>) loadedDoc.field("map")).get("Luca");
    Assert.assertEquals(newDoc.field("name"), "Luca");

    newDoc = ((Map<String, ODocument>) loadedDoc.field("map")).get("Marcus");
    Assert.assertEquals(newDoc.field("name"), "Marcus");

    newDoc = ((Map<String, ODocument>) loadedDoc.field("map")).get("Cesare");
    Assert.assertEquals(newDoc.field("name"), "Cesare");
  }

  @Test
  public void testListToJSON() {
    final List<ODocument> list = new ArrayList<ODocument>();
    final ODocument first = new ODocument().field("name", "Luca");
    final ODocument second = new ODocument().field("name", "Marcus");
    list.add(first);
    list.add(second);

    final String jsonResult = OJSONWriter.listToJSON(list, null);
    final ODocument doc = new ODocument();
    doc.fromJSON("{\"result\": " + jsonResult + "}");
    Collection<ODocument> result = doc.field("result");
    Assert.assertTrue(result instanceof Collection);
    Assert.assertEquals(result.size(), 2);
    for (final ODocument resultDoc : result) {
      Assert.assertTrue(first.hasSameContentOf(resultDoc) || second.hasSameContentOf(resultDoc));
    }
  }

  @Test
  public void testEmptyEmbeddedMap() {
    final ODocument doc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    doc.field("embeddedMap", map, OType.EMBEDDEDMAP);

    final String json = doc.toJSON();
    final ODocument loadedDoc = new ODocument().fromJSON(json);
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);

    final Map<String, ODocument> loadedMap = loadedDoc.field("embeddedMap");
    Assert.assertEquals(loadedMap.size(), 0);
  }

  @Test
  public void testMultiLevelTypes() {
    String oldDataTimeFormat = database.get(ODatabase.ATTRIBUTES.DATETIMEFORMAT).toString();
    database.set(
        ODatabase.ATTRIBUTES.DATETIMEFORMAT, OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
    try {
      ODocument newDoc = new ODocument();
      newDoc.field("long", 100000000000l);
      newDoc.field("date", new Date());
      newDoc.field("byte", (byte) 12);
      ODocument firstLevelDoc = new ODocument();
      firstLevelDoc.field("long", 200000000000l);
      firstLevelDoc.field("date", new Date());
      firstLevelDoc.field("byte", (byte) 13);
      ODocument secondLevelDoc = new ODocument();
      secondLevelDoc.field("long", 300000000000l);
      secondLevelDoc.field("date", new Date());
      secondLevelDoc.field("byte", (byte) 14);
      ODocument thirdLevelDoc = new ODocument();
      thirdLevelDoc.field("long", 400000000000l);
      thirdLevelDoc.field("date", new Date());
      thirdLevelDoc.field("byte", (byte) 15);
      newDoc.field("doc", firstLevelDoc);
      firstLevelDoc.field("doc", secondLevelDoc);
      secondLevelDoc.field("doc", thirdLevelDoc);

      final String json = newDoc.toJSON();
      ODocument loadedDoc = new ODocument().fromJSON(json);

      Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));
      Assert.assertTrue(loadedDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) newDoc.field("long")).longValue(), ((Long) loadedDoc.field("long")).longValue());
      Assert.assertTrue(loadedDoc.field("date") instanceof Date);
      Assert.assertTrue(loadedDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) newDoc.field("byte")).byteValue(), ((Byte) loadedDoc.field("byte")).byteValue());
      Assert.assertTrue(loadedDoc.field("doc") instanceof ODocument);

      ODocument firstDoc = loadedDoc.field("doc");
      Assert.assertTrue(firstLevelDoc.hasSameContentOf(firstDoc));
      Assert.assertTrue(firstDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) firstLevelDoc.field("long")).longValue(),
          ((Long) firstDoc.field("long")).longValue());
      Assert.assertTrue(firstDoc.field("date") instanceof Date);
      Assert.assertTrue(firstDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) firstLevelDoc.field("byte")).byteValue(),
          ((Byte) firstDoc.field("byte")).byteValue());
      Assert.assertTrue(firstDoc.field("doc") instanceof ODocument);

      ODocument secondDoc = firstDoc.field("doc");
      Assert.assertTrue(secondLevelDoc.hasSameContentOf(secondDoc));
      Assert.assertTrue(secondDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) secondLevelDoc.field("long")).longValue(),
          ((Long) secondDoc.field("long")).longValue());
      Assert.assertTrue(secondDoc.field("date") instanceof Date);
      Assert.assertTrue(secondDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) secondLevelDoc.field("byte")).byteValue(),
          ((Byte) secondDoc.field("byte")).byteValue());
      Assert.assertTrue(secondDoc.field("doc") instanceof ODocument);

      ODocument thirdDoc = secondDoc.field("doc");
      Assert.assertTrue(thirdLevelDoc.hasSameContentOf(thirdDoc));
      Assert.assertTrue(thirdDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) thirdLevelDoc.field("long")).longValue(),
          ((Long) thirdDoc.field("long")).longValue());
      Assert.assertTrue(thirdDoc.field("date") instanceof Date);
      Assert.assertTrue(thirdDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) thirdLevelDoc.field("byte")).byteValue(),
          ((Byte) thirdDoc.field("byte")).byteValue());
    } finally {
      database.set(ODatabase.ATTRIBUTES.DATETIMEFORMAT, oldDataTimeFormat);
    }
  }

  @Test
  public void testMerge() {
    ODocument doc1 = new ODocument();
    final ArrayList<String> list = new ArrayList<String>();
    doc1.field("embeddedList", list, OType.EMBEDDEDLIST);
    list.add("Luca");
    list.add("Marcus");
    list.add("Jay");
    doc1.field("salary", 10000);
    doc1.field("years", 16);

    ODocument doc2 = new ODocument();
    final ArrayList<String> list2 = new ArrayList<String>();
    doc2.field("embeddedList", list2, OType.EMBEDDEDLIST);
    list2.add("Luca");
    list2.add("Michael");
    doc2.field("years", 32);

    ODocument docMerge1 = doc1.copy();
    docMerge1.merge(doc2, true, true);

    Assert.assertTrue(docMerge1.containsField("embeddedList"));
    Assert.assertTrue(docMerge1.field("embeddedList") instanceof List<?>);
    Assert.assertEquals(((List<String>) docMerge1.field("embeddedList")).size(), 4);
    Assert.assertTrue(((List<String>) docMerge1.field("embeddedList")).get(0) instanceof String);
    Assert.assertEquals(((Integer) docMerge1.field("salary")).intValue(), 10000);
    Assert.assertEquals(((Integer) docMerge1.field("years")).intValue(), 32);

    ODocument docMerge2 = doc1.copy();
    docMerge2.merge(doc2, true, false);

    Assert.assertTrue(docMerge2.containsField("embeddedList"));
    Assert.assertTrue(docMerge2.field("embeddedList") instanceof List<?>);
    Assert.assertEquals(((List<String>) docMerge2.field("embeddedList")).size(), 2);
    Assert.assertTrue(((List<String>) docMerge2.field("embeddedList")).get(0) instanceof String);
    Assert.assertEquals(((Integer) docMerge2.field("salary")).intValue(), 10000);
    Assert.assertEquals(((Integer) docMerge2.field("years")).intValue(), 32);

    ODocument docMerge3 = doc1.copy();

    doc2.removeField("years");
    docMerge3.merge(doc2, false, false);

    Assert.assertTrue(docMerge3.containsField("embeddedList"));
    Assert.assertTrue(docMerge3.field("embeddedList") instanceof List<?>);
    Assert.assertEquals(((List<String>) docMerge3.field("embeddedList")).size(), 2);
    Assert.assertTrue(((List<String>) docMerge3.field("embeddedList")).get(0) instanceof String);
    Assert.assertFalse(docMerge3.containsField("salary"));
    Assert.assertFalse(docMerge3.containsField("years"));
  }

  @Test
  public void testNestedEmbeddedMap() {
    ODocument newDoc = new ODocument();

    final Map<String, HashMap<?, ?>> map1 = new HashMap<String, HashMap<?, ?>>();
    newDoc.field("map1", map1, OType.EMBEDDEDMAP);

    final Map<String, HashMap<?, ?>> map2 = new HashMap<String, HashMap<?, ?>>();
    map1.put("map2", (HashMap<?, ?>) map2);

    final Map<String, HashMap<?, ?>> map3 = new HashMap<String, HashMap<?, ?>>();
    map2.put("map3", (HashMap<?, ?>) map3);

    String json = newDoc.toJSON();
    ODocument loadedDoc = new ODocument().fromJSON(json);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("map1"));
    Assert.assertTrue(loadedDoc.field("map1") instanceof Map<?, ?>);
    final Map<String, ODocument> loadedMap1 = loadedDoc.field("map1");
    Assert.assertEquals(loadedMap1.size(), 1);

    Assert.assertTrue(loadedMap1.containsKey("map2"));
    Assert.assertTrue(loadedMap1.get("map2") instanceof Map<?, ?>);
    final Map<String, ODocument> loadedMap2 = (Map<String, ODocument>) loadedMap1.get("map2");
    Assert.assertEquals(loadedMap2.size(), 1);

    Assert.assertTrue(loadedMap2.containsKey("map3"));
    Assert.assertTrue(loadedMap2.get("map3") instanceof Map<?, ?>);
    final Map<String, ODocument> loadedMap3 = (Map<String, ODocument>) loadedMap2.get("map3");
    Assert.assertEquals(loadedMap3.size(), 0);
  }

  @Test
  public void testFetchedJson() {
    OObjectDatabaseTx database = new OObjectDatabaseTx(url);
    database.open("admin", "admin");
    try {
      database
          .getEntityManager()
          .registerEntityClasses("com.orientechnologies.orient.test.domain.business");
      database
          .getEntityManager()
          .registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
      database
          .getEntityManager()
          .registerEntityClasses("com.orientechnologies.orient.test.domain.base");

      List<ODocument> result =
          database
              .getUnderlying()
              .command(
                  new OSQLSynchQuery<ODocument>(
                      "select * from Profile where name = 'Barack' and surname = 'Obama'"))
              .execute();

      for (ODocument doc : result) {
        String jsonFull =
            doc.toJSON("type,rid,version,class,keepTypes,attribSameRow,indent:0,fetchPlan:*:-1");
        ODocument loadedDoc = new ODocument().fromJSON(jsonFull);

        Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
      }
    } finally {
      database.close();
    }
  }

  @Test
  public void testToJSONWithNoLazyLoadAndClosedDatabase() {
    List<ODocument> result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "select * from Profile where name = 'Barack' and surname = 'Obama'"))
            .execute();

    for (ODocument doc : result) {
      doc.reload("*:0");
      String jsonFull = doc.toJSON();
      ORID rid = doc.getIdentity();
      database.close();
      database.open("admin", "admin");
      doc = database.load(rid);
      doc.setLazyLoad(false);
      doc.reload("*:0");
      database.close();
      String jsonLoaded = doc.toJSON();
      Assert.assertEquals(jsonLoaded, jsonFull);
      database.open("admin", "admin");
      doc = database.load(rid);
      doc.setLazyLoad(false);
      doc.load("*:0");
      database.close();
      jsonLoaded = doc.toJSON();

      Assert.assertEquals(jsonLoaded, jsonFull);
    }

    if (database.isClosed()) database.open("admin", "admin");

    for (ODocument doc : result) {
      doc.reload("*:1");
      String jsonFull = doc.toJSON();
      ORID rid = doc.getIdentity();
      database.close();
      database.open("admin", "admin");
      doc = database.load(rid);
      doc.setLazyLoad(false);
      doc.reload("*:1");
      database.close();
      String jsonLoaded = doc.toJSON();
      Assert.assertEquals(jsonFull, jsonLoaded);
      database.open("admin", "admin");
      doc = database.load(rid);
      doc.setLazyLoad(false);
      doc.load("*:1");
      database.close();
      jsonLoaded = doc.toJSON();

      Assert.assertEquals(jsonFull, jsonLoaded);
    }
  }

  // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
  public void testSpecialChar() {
    final ODocument doc =
        new ODocument()
            .fromJSON(
                "{name:{\"%Field\":[\"value1\",\"value2\"],\"%Field2\":{},\"%Field3\":\"value3\"}}");
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertEquals(doc, loadedDoc);
  }

  public void testArrayOfArray() {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 100,  0 ],  [ 101, 1 ] ]}");
    doc.save();
    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
  }

  public void testLongTypes() {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 32874387347347,  0 ],  [ -23736753287327, 1 ] ]}");
    doc.save();
    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
  }

  // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
  public void testSpecialChars() {
    final ODocument doc =
        new ODocument()
            .fromJSON(
                "{Field:{\"Key1\":[\"Value1\",\"Value2\"],\"Key2\":{\"%%dummy%%\":null},\"Key3\":\"Value3\"}}");
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertEquals(doc, loadedDoc);
  }

  public void testJsonToStream() {
    final String doc1Json =
        "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    final ODocument doc1 = new ODocument().fromJSON(doc1Json);
    final String doc1String = new String(ORecordSerializerSchemaAware2CSV.INSTANCE.toStream(doc1));
    Assert.assertEquals(doc1Json, "{" + doc1String + "}");

    final String doc2Json =
        "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    final ODocument doc2 = new ODocument().fromJSON(doc2Json);
    final String doc2String = new String(ORecordSerializerSchemaAware2CSV.INSTANCE.toStream(doc2));
    Assert.assertEquals(doc2Json, "{" + doc2String + "}");
  }

  public void testSameNameCollectionsAndMap() {
    ODocument doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    List<ODocument> list = new ArrayList<ODocument>();
    for (int i = 0; i < 1; i++) {
      final ODocument doc1 = new ODocument();
      doc.field("number", i);
      list.add(doc1);
      Map<String, ODocument> docMap = new HashMap<String, ODocument>();
      for (int j = 0; j < 1; j++) {
        ODocument doc2 = new ODocument();
        doc2.field("blabla", j);
        docMap.put(String.valueOf(j), doc2);
        ODocument doc3 = new ODocument();
        doc3.field("blubli", String.valueOf(i + j));
        doc2.field("out", doc3);
      }
      doc1.field("out", docMap);
      list.add(doc1);
    }
    doc.field("out", list);
    String json = doc.toJSON();
    ODocument newDoc = new ODocument().fromJSON(json);
    Assert.assertEquals(json, newDoc.toJSON());
    Assert.assertTrue(newDoc.hasSameContentOf(doc));

    doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    final Map<String, ODocument> docMap = new HashMap<String, ODocument>();
    for (int i = 0; i < 10; i++) {
      ODocument doc1 = new ODocument();
      doc.field("number", i);
      list.add(doc1);
      list = new ArrayList<>();
      for (int j = 0; j < 5; j++) {
        ODocument doc2 = new ODocument();
        doc2.field("blabla", j);
        list.add(doc2);
        ODocument doc3 = new ODocument();
        doc3.field("blubli", String.valueOf(i + j));
        doc2.field("out", doc3);
      }
      doc1.field("out", list);
      docMap.put(String.valueOf(i), doc1);
    }
    doc.field("out", docMap);
    json = doc.toJSON();
    newDoc = new ODocument().fromJSON(json);
    Assert.assertEquals(newDoc.toJSON(), json);
    Assert.assertTrue(newDoc.hasSameContentOf(doc));
  }

  public void testSameNameCollectionsAndMap2() {
    ODocument doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    List<ODocument> list = new ArrayList<ODocument>();
    for (int i = 0; i < 2; i++) {
      ODocument doc1 = new ODocument();
      list.add(doc1);
      Map<String, ODocument> docMap = new HashMap<String, ODocument>();
      for (int j = 0; j < 5; j++) {
        ODocument doc2 = new ODocument();
        doc2.field("blabla", j);
        docMap.put(String.valueOf(j), doc2);
      }
      doc1.field("theMap", docMap);
      list.add(doc1);
    }
    doc.field("theList", list);
    String json = doc.toJSON();
    ODocument newDoc = new ODocument().fromJSON(json);
    Assert.assertEquals(newDoc.toJSON(), json);
    Assert.assertTrue(newDoc.hasSameContentOf(doc));
  }

  public void testSameNameCollectionsAndMap3() {
    ODocument doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    List<Map<String, ODocument>> list = new ArrayList<Map<String, ODocument>>();
    for (int i = 0; i < 2; i++) {
      Map<String, ODocument> docMap = new HashMap<String, ODocument>();
      for (int j = 0; j < 5; j++) {
        ODocument doc1 = new ODocument();
        doc1.field("blabla", j);
        docMap.put(String.valueOf(j), doc1);
      }

      list.add(docMap);
    }
    doc.field("theList", list);
    String json = doc.toJSON();
    ODocument newDoc = new ODocument().fromJSON(json);
    Assert.assertEquals(newDoc.toJSON(), json);
  }

  public void testNestedJsonCollection() {
    if (!database.getMetadata().getSchema().existsClass("Device"))
      database.getMetadata().getSchema().createClass("Device");

    database
        .command(
            new OCommandSQL(
                "insert into device (resource_id, domainset) VALUES (0, [ { 'domain' : 'abc' }, { 'domain' : 'pqr' } ])"))
        .execute();

    List<ODocument> result =
        database.query(
            new OSQLSynchQuery<>("select from device where domainset.domain contains 'abc'"));
    Assert.assertTrue(result.size() > 0);

    result =
        database.query(
            new OSQLSynchQuery<>("select from device where domainset[domain = 'abc'] is not null"));
    Assert.assertTrue(result.size() > 0);

    result =
        database.query(
            new OSQLSynchQuery<>("select from device where domainset.domain contains 'pqr'"));
    Assert.assertTrue(result.size() > 0);
  }

  public void testNestedEmbeddedJson() {
    if (!database.getMetadata().getSchema().existsClass("Device"))
      database.getMetadata().getSchema().createClass("Device");

    database
        .command(
            new OCommandSQL(
                "insert into device (resource_id, domainset) VALUES (1, { 'domain' : 'eee' })"))
        .execute();

    List<ODocument> result =
        database.query(
            new OSQLSynchQuery<Object>("select from device where domainset.domain = 'eee'"));
    Assert.assertTrue(result.size() > 0);
  }

  public void testNestedMultiLevelEmbeddedJson() {
    if (!database.getMetadata().getSchema().existsClass("Device"))
      database.getMetadata().getSchema().createClass("Device");

    database
        .command(
            new OCommandSQL(
                "insert into device (domainset) values ({'domain' : { 'lvlone' : { 'value' : 'five' } } } )"))
        .execute();

    List<ODocument> result =
        database.query(
            new OSQLSynchQuery<>(
                "select from device where domainset.domain.lvlone.value = 'five'"));
    Assert.assertTrue(result.size() > 0);
  }

  public void testSpaces() {
    ODocument doc = new ODocument();
    String test =
        "{"
            + "\"embedded\": {"
            + "\"second_embedded\":  {"
            + "\"text\":\"this is a test\""
            + "}"
            + "}"
            + "}";
    doc.fromJSON(test);
    Assert.assertTrue(doc.toJSON("fetchPlan:*:0,rid").indexOf("this is a test") > -1);
  }

  public void testEscaping() {
    ODocument doc = new ODocument();
    String s =
        "{\"name\": \"test\", \"nested\": { \"key\": \"value\", \"anotherKey\": 123 }, \"deep\": {\"deeper\": { \"k\": \"v\",\"quotes\": \"\\\"\\\",\\\"oops\\\":\\\"123\\\"\", \"likeJson\": \"[1,2,3]\",\"spaces\": \"value with spaces\"}}}";
    doc.fromJSON(s);
    Assert.assertEquals(doc.field("deep[deeper][quotes]"), "\"\",\"oops\":\"123\"");

    String res = doc.toJSON();

    // LOOK FOR "quotes": \"\",\"oops\":\"123\"
    Assert.assertTrue(res.contains("\"quotes\":\"\\\"\\\",\\\"oops\\\":\\\"123\\\"\""));
  }

  public void testEscapingDoubleQuotes() {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append(
        " {\n"
            + "    \"foo\":{\n"
            + "            \"bar\":{\n"
            + "                \"P357\":[\n"
            + "                            {\n"
            + "\n"
            + "                                \"datavalue\":{\n"
            + "                                    \"value\":\"\\\"\\\"\" \n"
            + "                                }\n"
            + "                        }\n"
            + "                ]   \n"
            + "            },\n"
            + "            \"three\": \"a\"\n"
            + "        }\n"
            + "} ");
    doc.fromJSON(sb.toString());
    Assert.assertEquals(doc.field("foo.three"), "a");
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"\"");
  }

  public void testEscapingDoubleQuotes2() {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append(
        " {\n"
            + "    \"foo\":{\n"
            + "            \"bar\":{\n"
            + "                \"P357\":[\n"
            + "                            {\n"
            + "\n"
            + "                                \"datavalue\":{\n"
            + "                                    \"value\":\"\\\"\",\n"
            + "\n"
            + "                                }\n"
            + "                        }\n"
            + "                ]   \n"
            + "            },\n"
            + "            \"three\": \"a\"\n"
            + "        }\n"
            + "} ");

    doc.fromJSON(sb.toString());
    Assert.assertEquals(doc.field("foo.three"), "a");
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"");
  }

  public void testEscapingDoubleQuotes3() {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append(
        " {\n"
            + "    \"foo\":{\n"
            + "            \"bar\":{\n"
            + "                \"P357\":[\n"
            + "                            {\n"
            + "\n"
            + "                                \"datavalue\":{\n"
            + "                                    \"value\":\"\\\"\",\n"
            + "\n"
            + "                                }\n"
            + "                        }\n"
            + "                ]   \n"
            + "            }\n"
            + "        }\n"
            + "} ");

    doc.fromJSON(sb.toString());
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"");
  }

  public void testEmbeddedQuotes() {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    // FROM ISSUE 3151
    builder.append("{\"mainsnak\":{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}}");
    doc.fromJSON(builder.toString());
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Sub\\urban");
  }

  public void testEmbeddedQuotes2() {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}");
    doc.fromJSON(builder.toString());
    Assert.assertEquals(doc.field("datavalue.value"), "Sub\\urban");
  }

  public void testEmbeddedQuotes2a() {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":\"Sub\\\\urban\"}");
    doc.fromJSON(builder.toString());
    Assert.assertEquals(doc.field("datavalue"), "Sub\\urban");
  }

  public void testEmbeddedQuotes3() {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}}");
    doc.fromJSON(sb.toString());
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\\"");
  }

  public void testEmbeddedQuotes4() {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}");
    doc.fromJSON(sb.toString());
    Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\\"");
  }

  public void testEmbeddedQuotes5() {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"datavalue\":\"Suburban\\\\\"\"}");
    doc.fromJSON(sb.toString());
    Assert.assertEquals(doc.field("datavalue"), "Suburban\\\"");
  }

  public void testEmbeddedQuotes6() {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    builder.append("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"}}}");
    doc.fromJSON(builder.toString());
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\");
  }

  public void testEmbeddedQuotes7() {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":{\"value\":\"Suburban\\\\\"}}");
    doc.fromJSON(builder.toString());
    Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\");
  }

  public void testEmbeddedQuotes8() {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":\"Suburban\\\\\"}");
    doc.fromJSON(builder.toString());
    Assert.assertEquals(doc.field("datavalue"), "Suburban\\");
  }

  public void testEmpty() {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    builder.append("{}");
    doc.fromJSON(builder.toString());
    Assert.assertEquals(doc.fieldNames().length, 0);
  }

  public void testInvalidJson() {
    ODocument doc = new ODocument();
    try {
      doc.fromJSON("{");
      Assert.fail();
    } catch (OSerializationException e) {
    }

    try {
      doc.fromJSON("{\"foo\":{}");
      Assert.fail();
    } catch (OSerializationException e) {
    }

    try {
      doc.fromJSON("{{}");
      Assert.fail();
    } catch (OSerializationException e) {
    }

    try {
      doc.fromJSON("{}}");
      Assert.fail();
    } catch (OSerializationException e) {
    }

    try {
      doc.fromJSON("}");
      Assert.fail();
    } catch (OSerializationException e) {
    }
  }

  public void testDates() {
    final Date now = new Date(1350518475000l);

    final ODocument doc = new ODocument();
    doc.field("date", now);
    final String json = doc.toJSON();

    final ODocument unmarshalled = new ODocument().fromJSON(json);
    Assert.assertEquals(unmarshalled.field("date"), now);
  }

  @Test
  public void shouldDeserializeFieldWithCurlyBraces() {
    final String json = "{\"a\":\"{dd}\",\"bl\":{\"b\":\"c\",\"a\":\"d\"}}";
    final ODocument in =
        (ODocument)
            ORecordSerializerJSON.INSTANCE.fromString(
                json, database.newInstance(), new String[] {});
    Assert.assertEquals(in.field("a"), "{dd}");
    Assert.assertTrue(in.field("bl") instanceof Map);
  }

  @Test
  public void testList() throws Exception {
    ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [\"string\", 42]}");

    ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), "string");
    Assert.assertEquals(list.get(1), 42);
  }

  @Test
  public void testEmbeddedRIDBagDeserialisationWhenFieldTypeIsProvided() throws Exception {
    ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        "{FirstName:\"Student A 0\",in_EHasGoodStudents:[#57:0],@fieldTypes:\"in_EHasGoodStudents=g\"}");

    ORidBag bag = documentSource.field("in_EHasGoodStudents");
    Assert.assertEquals(bag.size(), 1);
    OIdentifiable rid = bag.rawIterator().next();
    Assert.assertTrue(rid.getIdentity().getClusterId() == 57);
    Assert.assertTrue(rid.getIdentity().getClusterPosition() == 0);
  }

  public void testNestedLinkCreation() {
    ODocument jaimeDoc = new ODocument("NestedLinkCreation");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();

    // The link between jaime and cersei is saved properly - the #2263 test case
    ODocument cerseiDoc = new ODocument("NestedLinkCreation");
    cerseiDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"cersei\",\"valonqar\":" + jaimeDoc.toJSON() + "}");
    cerseiDoc.save();

    // The link between jamie and tyrion is not saved properly
    ODocument tyrionDoc = new ODocument("NestedLinkCreation");
    tyrionDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\", \"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}}");
    tyrionDoc.save();

    final Map<ORID, ODocument> contentMap = new HashMap<ORID, ODocument>();

    ODocument jaime = new ODocument("NestedLinkCreation");
    jaime.field("name", "jaime");

    contentMap.put(jaimeDoc.getIdentity(), jaime);

    ODocument cersei = new ODocument("NestedLinkCreation");
    cersei.field("name", "cersei");
    cersei.field("valonqar", jaimeDoc.getIdentity());
    contentMap.put(cerseiDoc.getIdentity(), cersei);

    ODocument tyrion = new ODocument("NestedLinkCreation");
    tyrion.field("name", "tyrion");

    ODocument embeddedDoc = new ODocument();
    embeddedDoc.field("relationship", "brother");
    embeddedDoc.field("contact", jaimeDoc.getIdentity());
    tyrion.field("emergency_contact", embeddedDoc);

    contentMap.put(tyrionDoc.getIdentity(), tyrion);

    final Map<ORID, List<ORID>> traverseMap = new HashMap<ORID, List<ORID>>();
    List<ORID> jaimeTraverse = new ArrayList<ORID>();
    jaimeTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(jaimeDoc.getIdentity(), jaimeTraverse);

    List<ORID> cerseiTraverse = new ArrayList<ORID>();
    cerseiTraverse.add(cerseiDoc.getIdentity());
    cerseiTraverse.add(jaimeDoc.getIdentity());

    traverseMap.put(cerseiDoc.getIdentity(), cerseiTraverse);

    List<ORID> tyrionTraverse = new ArrayList<ORID>();
    tyrionTraverse.add(tyrionDoc.getIdentity());
    tyrionTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(tyrionDoc.getIdentity(), tyrionTraverse);

    for (ODocument o : database.browseClass("NestedLinkCreation")) {
      ODocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));

      List<ORID> traverse = traverseMap.remove(o.getIdentity());
      for (OIdentifiable id :
          new OSQLSynchQuery<ODocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }

      Assert.assertTrue(traverse.isEmpty());
    }

    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testNestedLinkCreationFieldTypes() {
    ODocument jaimeDoc = new ODocument("NestedLinkCreationFieldTypes");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();

    // The link between jaime and cersei is saved properly - the #2263 test case
    ODocument cerseiDoc = new ODocument("NestedLinkCreationFieldTypes");
    cerseiDoc.fromJSON(
        "{\"@type\":\"d\",\"@fieldTypes\":\"valonqar=x\",\"name\":\"cersei\",\"valonqar\":"
            + jaimeDoc.getIdentity()
            + "}");
    cerseiDoc.save();

    // The link between jamie and tyrion is not saved properly
    ODocument tyrionDoc = new ODocument("NestedLinkCreationFieldTypes");
    tyrionDoc.fromJSON(
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\", \"@fieldTypes\":\"contact=x\",\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.getIdentity()
            + "}}");
    tyrionDoc.save();

    final Map<ORID, ODocument> contentMap = new HashMap<ORID, ODocument>();

    ODocument jaime = new ODocument("NestedLinkCreationFieldTypes");
    jaime.field("name", "jaime");

    contentMap.put(jaimeDoc.getIdentity(), jaime);

    ODocument cersei = new ODocument("NestedLinkCreationFieldTypes");
    cersei.field("name", "cersei");
    cersei.field("valonqar", jaimeDoc.getIdentity());
    contentMap.put(cerseiDoc.getIdentity(), cersei);

    ODocument tyrion = new ODocument("NestedLinkCreationFieldTypes");
    tyrion.field("name", "tyrion");

    ODocument embeddedDoc = new ODocument();
    embeddedDoc.field("relationship", "brother");
    embeddedDoc.field("contact", jaimeDoc.getIdentity());
    tyrion.field("emergency_contact", embeddedDoc);

    contentMap.put(tyrionDoc.getIdentity(), tyrion);

    final Map<ORID, List<ORID>> traverseMap = new HashMap<ORID, List<ORID>>();
    List<ORID> jaimeTraverse = new ArrayList<ORID>();
    jaimeTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(jaimeDoc.getIdentity(), jaimeTraverse);

    List<ORID> cerseiTraverse = new ArrayList<ORID>();
    cerseiTraverse.add(cerseiDoc.getIdentity());
    cerseiTraverse.add(jaimeDoc.getIdentity());

    traverseMap.put(cerseiDoc.getIdentity(), cerseiTraverse);

    List<ORID> tyrionTraverse = new ArrayList<ORID>();
    tyrionTraverse.add(tyrionDoc.getIdentity());
    tyrionTraverse.add(jaimeDoc.getIdentity());
    traverseMap.put(tyrionDoc.getIdentity(), tyrionTraverse);

    for (ODocument o : database.browseClass("NestedLinkCreationFieldTypes")) {
      ODocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));

      List<ORID> traverse = traverseMap.remove(o.getIdentity());
      for (OIdentifiable id :
          new OSQLSynchQuery<ODocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }
      Assert.assertTrue(traverse.isEmpty());
    }
    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testInnerDocCreation() {
    ODocument adamDoc = new ODocument("InnerDocCreation");
    adamDoc.fromJSON("{\"name\":\"adam\"}");
    adamDoc.save();

    ODocument eveDoc = new ODocument("InnerDocCreation");
    eveDoc.fromJSON("{\"@type\":\"d\",\"name\":\"eve\",\"friends\":[" + adamDoc.toJSON() + "]}");
    eveDoc.save();

    Map<ORID, ODocument> contentMap = new HashMap<ORID, ODocument>();
    ODocument adam = new ODocument("InnerDocCreation");
    adam.field("name", "adam");

    contentMap.put(adamDoc.getIdentity(), adam);

    ODocument eve = new ODocument("InnerDocCreation");
    eve.field("name", "eve");

    List<ORID> friends = new ArrayList<ORID>();
    friends.add(adamDoc.getIdentity());
    eve.field("friends", friends);

    contentMap.put(eveDoc.getIdentity(), eve);

    Map<ORID, List<ORID>> traverseMap = new HashMap<ORID, List<ORID>>();

    List<ORID> adamTraverse = new ArrayList<ORID>();
    adamTraverse.add(adamDoc.getIdentity());
    traverseMap.put(adamDoc.getIdentity(), adamTraverse);

    List<ORID> eveTraverse = new ArrayList<ORID>();
    eveTraverse.add(eveDoc.getIdentity());
    eveTraverse.add(adamDoc.getIdentity());

    traverseMap.put(eveDoc.getIdentity(), eveTraverse);

    for (ODocument o : database.browseClass("InnerDocCreation")) {
      ODocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));
    }

    for (final ODocument o : database.browseClass("InnerDocCreation")) {
      final List<ORID> traverse = traverseMap.remove(o.getIdentity());
      for (final OIdentifiable id :
          new OSQLSynchQuery<ODocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }
      Assert.assertTrue(traverse.isEmpty());
    }
    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testInnerDocCreationFieldTypes() {
    ODocument adamDoc = new ODocument("InnerDocCreationFieldTypes");
    adamDoc.fromJSON("{\"name\":\"adam\"}");
    adamDoc.save();

    ODocument eveDoc = new ODocument("InnerDocCreationFieldTypes");
    eveDoc.fromJSON(
        "{\"@type\":\"d\", \"@fieldTypes\" : \"friends=z\", \"name\":\"eve\",\"friends\":["
            + adamDoc.getIdentity()
            + "]}");
    eveDoc.save();

    Map<ORID, ODocument> contentMap = new HashMap<ORID, ODocument>();
    ODocument adam = new ODocument("InnerDocCreationFieldTypes");
    adam.field("name", "adam");

    contentMap.put(adamDoc.getIdentity(), adam);

    ODocument eve = new ODocument("InnerDocCreationFieldTypes");
    eve.field("name", "eve");

    List<ORID> friends = new ArrayList<ORID>();
    friends.add(adamDoc.getIdentity());
    eve.field("friends", friends);

    contentMap.put(eveDoc.getIdentity(), eve);

    Map<ORID, List<ORID>> traverseMap = new HashMap<ORID, List<ORID>>();

    List<ORID> adamTraverse = new ArrayList<ORID>();
    adamTraverse.add(adamDoc.getIdentity());
    traverseMap.put(adamDoc.getIdentity(), adamTraverse);

    List<ORID> eveTraverse = new ArrayList<ORID>();
    eveTraverse.add(eveDoc.getIdentity());
    eveTraverse.add(adamDoc.getIdentity());

    traverseMap.put(eveDoc.getIdentity(), eveTraverse);

    for (ODocument o : database.browseClass("InnerDocCreationFieldTypes")) {
      ODocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));
    }

    for (ODocument o : database.browseClass("InnerDocCreationFieldTypes")) {
      List<ORID> traverse = traverseMap.remove(o.getIdentity());
      for (OIdentifiable id :
          new OSQLSynchQuery<ODocument>("traverse * from " + o.getIdentity().toString())) {
        Assert.assertTrue(traverse.remove(id.getIdentity()));
      }

      Assert.assertTrue(traverse.isEmpty());
    }

    Assert.assertTrue(traverseMap.isEmpty());
  }

  public void testJSONTxDocInsertOnly() {
    final String classNameDocOne = "JSONTxDocOneInsertOnly";
    if (!database.getMetadata().getSchema().existsClass(classNameDocOne)) {
      database.getMetadata().getSchema().createClass(classNameDocOne);
    }
    final String classNameDocTwo = "JSONTxDocTwoInsertOnly";
    if (!database.getMetadata().getSchema().existsClass(classNameDocTwo)) {
      database.getMetadata().getSchema().createClass(classNameDocTwo);
    }
    database.begin();
    final ODocument eveDoc = new ODocument(classNameDocOne);
    eveDoc.field("name", "eve");
    eveDoc.save();

    final ODocument nestedWithTypeD = new ODocument(classNameDocTwo);
    nestedWithTypeD.fromJSON(
        "{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":[" + eveDoc.toJSON() + "]}");
    nestedWithTypeD.save();
    database.commit();
    Assert.assertEquals(database.countClass(classNameDocOne), 1);

    final Map<ORID, ODocument> contentMap = new HashMap<>();
    final ODocument eve = new ODocument(classNameDocOne);
    eve.field("name", "eve");
    contentMap.put(eveDoc.getIdentity(), eve);

    for (final ODocument document : database.browseClass(classNameDocOne)) {
      final ODocument content = contentMap.get(document.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(document));
    }
  }

  public void testJSONTxDoc() {
    if (!database.getMetadata().getSchema().existsClass("JSONTxDocOne"))
      database.getMetadata().getSchema().createClass("JSONTxDocOne");

    if (!database.getMetadata().getSchema().existsClass("JSONTxDocTwo"))
      database.getMetadata().getSchema().createClass("JSONTxDocTwo");

    ODocument adamDoc = new ODocument("JSONTxDocOne");
    adamDoc.field("name", "adam");
    adamDoc.save();

    database.begin();
    ODocument eveDoc = new ODocument("JSONTxDocOne");
    eveDoc.field("name", "eve");
    eveDoc.save();

    final ODocument nestedWithTypeD = new ODocument("JSONTxDocTwo");
    nestedWithTypeD.fromJSON(
        "{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":["
            + eveDoc.toJSON()
            + ","
            + adamDoc.toJSON()
            + "]}");
    nestedWithTypeD.save();

    database.commit();

    Assert.assertEquals(database.countClass("JSONTxDocOne"), 2);

    Map<ORID, ODocument> contentMap = new HashMap<>();
    ODocument adam = new ODocument("JSONTxDocOne");
    adam.field("name", "adam");
    contentMap.put(adamDoc.getIdentity(), adam);

    ODocument eve = new ODocument("JSONTxDocOne");
    eve.field("name", "eve");
    contentMap.put(eveDoc.getIdentity(), eve);

    for (ODocument o : database.browseClass("JSONTxDocOne")) {
      ODocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));
    }
  }

  public void testInvalidLink() {
    ODocument nullRefDoc = new ODocument();
    nullRefDoc.fromJSON("{\"name\":\"Luca\", \"ref\":\"#-1:-1\"}");
    // Assert.assertNull(nullRefDoc.rawField("ref"));

    String json = nullRefDoc.toJSON();
    int pos = json.indexOf("\"ref\":");

    Assert.assertTrue(pos > -1);
    Assert.assertEquals(json.charAt(pos + "\"ref\":".length()), 'n');
  }

  public void testOtherJson() {
    new ODocument()
        .fromJSON(
            "{\"Salary\":1500.0,\"Type\":\"Person\",\"Address\":[{\"Zip\":\"JX2 MSX\",\"Type\":\"Home\",\"Street1\":\"13 Marge Street\",\"Country\":\"Holland\",\"Id\":\"Address-28813211\",\"City\":\"Amsterdam\",\"From\":\"1996-02-01\",\"To\":\"1998-01-01\"},{\"Zip\":\"90210\",\"Type\":\"Work\",\"Street1\":\"100 Hollywood Drive\",\"Country\":\"USA\",\"Id\":\"Address-11595040\",\"City\":\"Los Angeles\",\"From\":\"2009-09-01\"}],\"Id\":\"Person-7464251\",\"Name\":\"Stan\"}");
  }

  @Test
  public void testScientificNotation() {
    final ODocument doc = new ODocument();
    doc.fromJSON("{'number1': -9.2741500e-31, 'number2': 741800E+290}");

    final double number1 = doc.field("number1");
    Assert.assertEquals(number1, -9.27415E-31);
    final double number2 = doc.field("number2");
    Assert.assertEquals(number2, 741800E+290);
  }
}
