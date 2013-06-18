/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;

@SuppressWarnings("unchecked")
@Test
public class JSONTest {
  private String url;

  @Parameters(value = "url")
  public JSONTest(final String iURL) {
    url = iURL;
  }

  @Test
  public void testAlmostLink() {
    ODocument doc = new ODocument();
    doc.fromJSON("{'title': '#330: Dollar Coins Are Done'}");
  }

  @Test
  public void testNullList() throws Exception {
    ODocument documentSource = new ODocument();
    documentSource.fromJSON("{\"list\" : [\"string\", null]}");

    ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), "string");
    Assert.assertNull(list.get(1));
  }
  
  @Test
  public void testNullity() {
    ODocument newDoc = new ODocument();

    newDoc.fromJSON("{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\","
        + "\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith Ave\","
        + "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"}," + "\"dob\":\"2011-11-17T03:17:04Z\"}");

    String json = newDoc.toJSON();
    ODocument loadedDoc = new ODocument().fromJSON(json);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));
  }

  @Test
  public void testEmbeddedList() {
    ODocument newDoc = new ODocument();

    final ArrayList<ODocument> list = new ArrayList<ODocument>();
    newDoc.field("embeddedList", list, OType.EMBEDDEDLIST);
    list.add(new ODocument().field("name", "Luca"));
    list.add(new ODocument().field("name", "Marcus"));

    String json = newDoc.toJSON();
    ODocument loadedDoc = new ODocument().fromJSON(json);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("embeddedList"));
    Assert.assertTrue(loadedDoc.field("embeddedList") instanceof List<?>);
    Assert.assertTrue(((List<ODocument>) loadedDoc.field("embeddedList")).get(0) instanceof ODocument);

    ODocument d = ((List<ODocument>) loadedDoc.field("embeddedList")).get(0);
    Assert.assertEquals(d.field("name"), "Luca");
    d = ((List<ODocument>) loadedDoc.field("embeddedList")).get(1);
    Assert.assertEquals(d.field("name"), "Marcus");
  }

  @Test
  public void testListToJSON() {

    final ArrayList<ODocument> list = new ArrayList<ODocument>();
    ODocument first = new ODocument().field("name", "Luca");
    ODocument second = new ODocument().field("name", "Marcus");
    list.add(first);
    list.add(second);

    String jsonResult = OJSONWriter.listToJSON(list, null);
    ODocument doc = new ODocument();
    doc.fromJSON("{\"result\": " + jsonResult + "}");
    Collection<ODocument> result = doc.field("result");
    Assert.assertTrue(result instanceof Collection);
    Assert.assertEquals(result.size(), 2);
    for (ODocument resultDoc : result) {
      Assert.assertTrue(first.hasSameContentOf(resultDoc) || second.hasSameContentOf(resultDoc));
    }
  }

  @Test
  public void testEmptyEmbeddedMap() {
    ODocument newDoc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    newDoc.field("embeddedMap", map, OType.EMBEDDEDMAP);

    String json = newDoc.toJSON();
    ODocument loadedDoc = new ODocument().fromJSON(json);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);

    final Map<String, ODocument> loadedMap = loadedDoc.field("embeddedMap");
    Assert.assertEquals(loadedMap.size(), 0);
  }

  @Test
  public void testEmbeddedMap() {
    ODocument newDoc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    newDoc.field("map", map);
    map.put("Luca", new ODocument().field("name", "Luca"));
    map.put("Marcus", new ODocument().field("name", "Marcus"));
    map.put("Cesare", new ODocument().field("name", "Cesare"));

    String json = newDoc.toJSON();
    ODocument loadedDoc = new ODocument().fromJSON(json);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));

    Assert.assertTrue(loadedDoc.containsField("map"));
    Assert.assertTrue(loadedDoc.field("map") instanceof Map<?, ?>);
    Assert.assertTrue(((Map<String, ODocument>) loadedDoc.field("map")).values().iterator().next() instanceof ODocument);

    ODocument d = ((Map<String, ODocument>) loadedDoc.field("map")).get("Luca");
    Assert.assertEquals(d.field("name"), "Luca");

    d = ((Map<String, ODocument>) loadedDoc.field("map")).get("Marcus");
    Assert.assertEquals(d.field("name"), "Marcus");

    d = ((Map<String, ODocument>) loadedDoc.field("map")).get("Cesare");
    Assert.assertEquals(d.field("name"), "Cesare");
  }

  @Test
  public void testMultiLevelTypes() {
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

    String json = newDoc.toJSON();
    ODocument loadedDoc = new ODocument().fromJSON(json);

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));
    Assert.assertTrue(loadedDoc.field("long") instanceof Long);
    Assert.assertEquals(((Long) newDoc.field("long")).longValue(), ((Long) loadedDoc.field("long")).longValue());
    Assert.assertTrue(loadedDoc.field("date") instanceof Date);
    Assert.assertTrue(loadedDoc.field("byte") instanceof Byte);
    Assert.assertEquals(((Byte) newDoc.field("byte")).byteValue(), ((Byte) loadedDoc.field("byte")).byteValue());
    Assert.assertTrue(loadedDoc.field("doc") instanceof ODocument);

    ODocument firstDoc = loadedDoc.field("doc");
    Assert.assertTrue(firstLevelDoc.hasSameContentOf(firstDoc));
    Assert.assertTrue(firstDoc.field("long") instanceof Long);
    Assert.assertEquals(((Long) firstLevelDoc.field("long")).longValue(), ((Long) firstDoc.field("long")).longValue());
    Assert.assertTrue(firstDoc.field("date") instanceof Date);
    Assert.assertTrue(firstDoc.field("byte") instanceof Byte);
    Assert.assertEquals(((Byte) firstLevelDoc.field("byte")).byteValue(), ((Byte) firstDoc.field("byte")).byteValue());
    Assert.assertTrue(firstDoc.field("doc") instanceof ODocument);

    ODocument secondDoc = firstDoc.field("doc");
    Assert.assertTrue(secondLevelDoc.hasSameContentOf(secondDoc));
    Assert.assertTrue(secondDoc.field("long") instanceof Long);
    Assert.assertEquals(((Long) secondLevelDoc.field("long")).longValue(), ((Long) secondDoc.field("long")).longValue());
    Assert.assertTrue(secondDoc.field("date") instanceof Date);
    Assert.assertTrue(secondDoc.field("byte") instanceof Byte);
    Assert.assertEquals(((Byte) secondLevelDoc.field("byte")).byteValue(), ((Byte) secondDoc.field("byte")).byteValue());
    Assert.assertTrue(secondDoc.field("doc") instanceof ODocument);

    ODocument thirdDoc = secondDoc.field("doc");
    Assert.assertTrue(thirdLevelDoc.hasSameContentOf(thirdDoc));
    Assert.assertTrue(thirdDoc.field("long") instanceof Long);
    Assert.assertEquals(((Long) thirdLevelDoc.field("long")).longValue(), ((Long) thirdDoc.field("long")).longValue());
    Assert.assertTrue(thirdDoc.field("date") instanceof Date);
    Assert.assertTrue(thirdDoc.field("byte") instanceof Byte);
    Assert.assertEquals(((Byte) thirdLevelDoc.field("byte")).byteValue(), ((Byte) thirdDoc.field("byte")).byteValue());
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
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");

    List<ODocument> result = database.getUnderlying()
        .command(new OSQLSynchQuery<ODocument>("select * from Profile where name = 'Barack' and surname = 'Obama'")).execute();

    for (ODocument doc : result) {
      String jsonFull = doc.toJSON("type,rid,version,class,attribSameRow,indent:0,fetchPlan:*:-1");
      ODocument loadedDoc = new ODocument().fromJSON(jsonFull);

      Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
    }
  }

  @Test
  public void testToJSONWithNoLazyLoadAndClosedDatabase() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select * from Profile where name = 'Barack' and surname = 'Obama'")).execute();

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

    database.open("admin", "admin");

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

  public void testSpecialChar() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODocument doc = new ODocument().fromJSON("{name:{\"%Field\":[\"value1\",\"value2\"],\"%Field2\":{},\"%Field3\":\"value3\"}}");
    doc.save();

    ODocument doc2 = database.load(doc.getIdentity());
    Assert.assertEquals(doc, doc2);
  }

  public void testArrayOfArray() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODocument newDoc = new ODocument();

    newDoc
        .fromJSON("{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 100,  0 ],  [ 101, 1 ] ]}");

    newDoc.save();

    ODocument loadedDoc = database.load(newDoc.getIdentity());

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));
    database.close();
  }

  public void testLongTypes() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODocument newDoc = new ODocument();

    newDoc
        .fromJSON("{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 32874387347347,  0 ],  [ -23736753287327, 1 ] ]}");

    newDoc.save();

    ODocument loadedDoc = database.load(newDoc.getIdentity());

    Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc));
    database.close();
  }

  public void testSpecialChars() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODocument doc = new ODocument()
        .fromJSON("{Field:{\"Key1\":[\"Value1\",\"Value2\"],\"Key2\":{\"%%dummy%%\":null},\"Key3\":\"Value3\"}}");
    doc.save();

    ODocument doc2 = database.load(doc.getIdentity());
    Assert.assertEquals(doc, doc2);
  }

  public void testJsonToStream() {
    String doc1Json = "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    ODocument doc1 = new ODocument().fromJSON(doc1Json);
    String doc1String = new String(doc1.toStream());
    Assert.assertEquals(doc1Json, "{" + doc1String + "}");

    String doc2Json = "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    ODocument doc2 = new ODocument().fromJSON(doc2Json);
    String doc2String = new String(doc2.toStream());
    Assert.assertEquals(doc2Json, "{" + doc2String + "}");
  }

  public void testSameNameCollectionsAndMap() {
    ODocument doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    List<ODocument> list = new ArrayList<ODocument>();
    for (int i = 0; i < 10; i++) {
      ODocument doc1 = new ODocument();
      doc.field("number", i);
      list.add(doc1);
      Map<String, ODocument> docMap = new HashMap<String, ODocument>();
      for (int j = 0; j < 5; j++) {
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
    Assert.assertEquals(newDoc.toJSON(), json);
    Assert.assertTrue(newDoc.hasSameContentOf(doc));

    doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    Map<String, ODocument> docMap = new HashMap<String, ODocument>();
    for (int i = 0; i < 10; i++) {
      ODocument doc1 = new ODocument();
      doc.field("number", i);
      list.add(doc1);
      list = new ArrayList<ODocument>();
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

  public void testNestedJsonCollection() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    try {
      if (!database.getMetadata().getSchema().existsClass("Device"))
        database.getMetadata().getSchema().createClass("Device");

      database
          .command(
              new OCommandSQL(
                  "insert into device (resource_id, domainset) VALUES (0, [ { 'domain' : 'abc' }, { 'domain' : 'pqr' } ])"))
          .execute();

      List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select from device where domainset.domain in 'abc'"));
      Assert.assertTrue(result.size() > 0);

      result = database.query(new OSQLSynchQuery<Object>("select from device where domainset[domain = 'abc'] is not null"));
      Assert.assertTrue(result.size() > 0);

      result = database.query(new OSQLSynchQuery<Object>("select from device where domainset.domain in 'pqr'"));
      Assert.assertTrue(result.size() > 0);

    } finally {
      database.close();
    }
  }

  public void testNestedEmbeddedJson() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    try {
      if (!database.getMetadata().getSchema().existsClass("Device"))
        database.getMetadata().getSchema().createClass("Device");

      database.command(new OCommandSQL("insert into device (resource_id, domainset) VALUES (1, { 'domain' : 'eee' })")).execute();

      List<ODocument> result = database
          .query(new OSQLSynchQuery<Object>("select from device where domainset.domain[name] in 'eee'"));
      Assert.assertTrue(result.size() > 0);

    } finally {
      database.close();
    }
  }

  public void testNestedMultiLevelEmbeddedJson() {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    try {
      if (!database.getMetadata().getSchema().existsClass("Device"))
        database.getMetadata().getSchema().createClass("Device");

      database.command(
          new OCommandSQL("insert into device (domainset) values ({'domain' : { 'lvlone' : { 'value' : 'five' } } } )")).execute();

      List<ODocument> result = database.query(new OSQLSynchQuery<Object>(
          "select from device where domainset.domain.lvlone.value in 'five'"));
      Assert.assertTrue(result.size() > 0);

    } finally {
      database.close();
    }
  }

  public void testSpaces() {
    ODocument doc = new ODocument();
    String test = "{" + "\"embedded\": {" + "\"second_embedded\":  {" + "\"text\":\"this is a test\"" + "}" + "}" + "}";
    doc.fromJSON(test);
    Assert.assertTrue(doc.toJSON("fetchPlan:*:0,rid").indexOf("this is a test") > -1);
  }

  public void testEscaping() {
    ODocument doc = new ODocument();
    String s = "{\"name\": \"test\", \"nested\": { \"key\": \"value\", \"anotherKey\": 123 }, \"deep\": {\"deeper\": { \"k\": \"v\",\"quotes\": \"\\\"\\\",\\\"oops\\\":\\\"123\\\"\", \"likeJson\": \"[1,2,3]\",\"spaces\": \"value with spaces\"}}}";
    doc.fromJSON(s);
    Assert.assertEquals(doc.field("deep[deeper][quotes]"), "\"\",\"oops\":\"123\"");

    String res = doc.toJSON();

    // LOOK FOR "quotes": \"\",\"oops\":\"123\"
    Assert.assertTrue(res.contains("\"quotes\":\"\\\"\\\",\\\"oops\\\":\\\"123\\\"\""));
  }

  public void testDates() {
    Date now = new Date(1350518475000l);

    ODocument doc = new ODocument();
    doc.field("date", now);
    String json = doc.toJSON();

    ODocument unmarshalled = new ODocument().fromJSON(json);
    Assert.assertEquals(unmarshalled.field("date"), now);
  }

  @Test
  public void shouldDeserializeFieldWithCurlyBraces() {
    ODatabaseDocumentTx tx = new ODatabaseDocumentTx("memory:test").create();

    String json = "{\"a\":\"{dd}\",\"bl\":{\"b\":\"c\",\"a\":\"d\"}}";
    ODocument in = (ODocument) ORecordSerializerJSON.INSTANCE.fromString(json, tx.newInstance(), new String[] {});

    Assert.assertEquals(in.field("a"), "{dd}");
    Assert.assertTrue(in.field("bl") instanceof Map);

    tx.drop();
  }

  @Test
  public void mapTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:test");
    db.create();

    ODocument doc = new ODocument("TestModel");
    doc.fromJSON("{\"@rid\":\"\",\"knows\":{\"#8:0\":{\"@rid\":\"#8:0\",\"relationship\":\"family\"}}}");
    doc.save();

    ODocument doc2 = new ODocument("TestModel");
    doc2.fromJSON("{\"@rid\":\"\",\"knows\":{\"#8:0\":{\"@rid\":\"bush\",\"relationship\":\"family\"}}}");
    doc2.save();

    for (ODocument o : db.browseClass("TestModel")) {
      System.out.println(o.toJSON());
    }

    db.drop();
  }

  @Test
  public void nestedJsonTest() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:test");
    db.create();

    ODocument rdoc = new ODocument("TestModel");
    rdoc.fromJSON("{\"@rid\":\"\",\"name\":\"Fox Trot\",\"knowledge\":[]}");
    rdoc.save();

    for (ODocument o : db.browseClass("TestModel"))
      System.out.println(o.toJSON());
    System.out.println("--------------------");

    ODocument jdoc = new ODocument("TestModel");
    jdoc.fromJSON("{\"name\":\"Jane Doe\",\"knowledge\":[{\"endNode\":\"#9:0\",\"relationship\":\"friend\",\"since\":\"2013-04-27T05:09:07.440Z\"}]}");
    jdoc.save();

    for (ODocument o : db.browseClass("TestModel"))
      System.out.println(o.toJSON());
    System.out.println("--------------------");

    db.command(
        new OCommandSQL(
            "UPDATE #9:0 merge {\"knowledge\":[{\"endNode\":\"#9:1\",\"relationship\":\"friend\",\"years\":0,\"since\":\"2013-04-27T16:07:15.094Z\"}]}"))
        .execute();

    for (ODocument o : db.browseClass("TestModel"))
      System.out.println(o.toJSON());
    System.out.println("--------------------");

    db.command(
        new OCommandSQL(
            "UPDATE #9:0 merge {\"knowledge\":[{\"endNode\":\"#9:1\",\"relationship\":\"friend\",\"years\":0,\"since\":\"2013-04-27T16:07:15.094Z\"}]}"))
        .execute();

    for (ODocument o : db.browseClass("TestModel"))
      System.out.println(o.toJSON());
    System.out.println("--------------------");

    db.command(
        new OCommandSQL(
            "Insert into TestModel content {\"name\":\"Theon Greyjoy\",\"knowledge\":[{\"endNode\":\"#9:1\",\"relationship\":\"friend\",\"since\":\"2013-04-27T05:09:07.440Z\"}]}"))
        .execute();

    for (ODocument o : db.browseClass("TestModel"))
      System.out.println(o.toJSON());
    System.out.println("--------------------");

    db.close();
  }
}
