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
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
public class JSONStreamTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public JSONStreamTest(@Optional final String iURL) {
    super(iURL);
  }

  @Test
  public void testAlmostLink() throws IOException {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        new ByteArrayInputStream(
            "{'title': '#330: Dollar Coins Are Done'}".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testNullList() throws Exception {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream(
            "{\"list\" : [\"string\", null]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), "string");
    Assert.assertNull(list.get(1));
  }

  @Test
  public void testBooleanList() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream("{\"list\" : [true, false]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), true);
    Assert.assertEquals(list.get(1), false);
  }

  @Test
  public void testNumericIntegerList() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream("{\"list\" : [17,42]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 17);
    Assert.assertEquals(list.get(1), 42);
  }

  @Test
  public void testNumericLongList() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream(
            "{\"list\" : [100000000000,100000000001]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 100000000000l);
    Assert.assertEquals(list.get(1), 100000000001l);
  }

  @Test
  public void testNumericFloatList() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream("{\"list\" : [17.3,42.7]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), 17.3);
    Assert.assertEquals(list.get(1), 42.7);
  }

  @Test
  public void testNullity() throws IOException {
    final ODocument doc = new ODocument();
    final String docString =
        "{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\","
            + "\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith Ave\","
            + "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"},"
            + "\"dob\":\"2011-11-17 03:17:04\"}";
    doc.fromJSON(new ByteArrayInputStream(docString.getBytes(StandardCharsets.UTF_8)));
    final String json = doc.toJSON();
    final ODocument loadedDoc = new ODocument().fromJSON(json);
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
  }

  // no benchmark
  @Test
  public void testEmbeddedList() throws IOException {
    final ODocument doc = new ODocument();
    final List<ODocument> list = new ArrayList<ODocument>();
    doc.field("embeddedList", list, OType.EMBEDDEDLIST);
    list.add(new ODocument().field("name", "Luca"));
    list.add(new ODocument().field("name", "Marcus"));

    final String json = doc.toJSON();
    final ODocument loadedDoc =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
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

  // no benchmark
  @Test
  public void testEmbeddedMap() throws IOException {
    final ODocument doc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    doc.field("map", map);
    map.put("Luca", new ODocument().field("name", "Luca"));
    map.put("Marcus", new ODocument().field("name", "Marcus"));
    map.put("Cesare", new ODocument().field("name", "Cesare"));

    final String json = doc.toJSON();
    final ODocument loadedDoc =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

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

  // no benchmark
  @Test
  public void testListToJSON() throws IOException {
    final List<ODocument> list = new ArrayList<ODocument>();
    final ODocument first = new ODocument().field("name", "Luca");
    final ODocument second = new ODocument().field("name", "Marcus");
    list.add(first);
    list.add(second);

    final String jsonResult = OJSONWriter.listToJSON(list, null);
    final ODocument doc = new ODocument();
    final String docString = "{\"result\": " + jsonResult + "}";
    doc.fromJSON(new ByteArrayInputStream(docString.getBytes(StandardCharsets.UTF_8)));
    final Collection<ODocument> result = doc.field("result");
    Assert.assertTrue(result instanceof Collection);
    Assert.assertEquals(result.size(), 2);
    for (final ODocument resultDoc : result) {
      Assert.assertTrue(first.hasSameContentOf(resultDoc) || second.hasSameContentOf(resultDoc));
    }
  }

  // no benchmark
  @Test
  public void testEmptyEmbeddedMap() throws IOException {
    final ODocument doc = new ODocument();

    final Map<String, ODocument> map = new HashMap<String, ODocument>();
    doc.field("embeddedMap", map, OType.EMBEDDEDMAP);

    final String json = doc.toJSON();
    final ODocument loadedDoc =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
    Assert.assertTrue(loadedDoc.containsField("embeddedMap"));
    Assert.assertTrue(loadedDoc.field("embeddedMap") instanceof Map<?, ?>);

    final Map<String, ODocument> loadedMap = loadedDoc.field("embeddedMap");
    Assert.assertEquals(loadedMap.size(), 0);
  }

  // TODO: "@fieldTypes":"date=t,byte=b,long=l"
  @Test
  public void testMultiLevelTypes() throws IOException {
    final String oldDataTimeFormat = database.get(ODatabase.ATTRIBUTES.DATETIMEFORMAT).toString();
    database.set(
        ODatabase.ATTRIBUTES.DATETIMEFORMAT, OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
    try {
      final ODocument doc = new ODocument();
      doc.field("long", 100000000000l);
      doc.field("date", new Date());
      doc.field("byte", (byte) 12);

      final ODocument firstLevelDoc = new ODocument();
      firstLevelDoc.field("long", 200000000000l);
      firstLevelDoc.field("date", new Date());
      firstLevelDoc.field("byte", (byte) 13);

      final ODocument secondLevelDoc = new ODocument();
      secondLevelDoc.field("long", 300000000000l);
      secondLevelDoc.field("date", new Date());
      secondLevelDoc.field("byte", (byte) 14);

      final ODocument thirdLevelDoc = new ODocument();
      thirdLevelDoc.field("long", 400000000000l);
      thirdLevelDoc.field("date", new Date());
      thirdLevelDoc.field("byte", (byte) 15);
      doc.field("doc", firstLevelDoc);
      firstLevelDoc.field("doc", secondLevelDoc);
      secondLevelDoc.field("doc", thirdLevelDoc);

      // TODO: WIP move @fileTypes from end to signature:
      // final String json =
      //
      // "{\"@type\":\"d\",\"@version\":0,\"@fieldTypes\":\"date=t,byte=b,long=l\",\"date\":\"2021-03-11 14:26:13:141\",\"byte\":12,\"doc\":{\"@type\":\"d\",\"@version\":0,\"@fieldTypes\":\"date=t,byte=b,long=l\",\"date\":\"2021-03-11 14:26:13:141\",\"byte\":13,\"doc\":{\"@type\":\"d\",\"@version\":0,\"@fieldTypes\":\"date=t,byte=b,long=l\",\"date\":\"2021-03-11 14:26:13:141\",\"byte\":14,\"doc\":{\"@type\":\"d\",\"@version\":0,\"@fieldTypes\":\"date=t,byte=b,long=l\",\"date\":\"2021-03-11 14:26:13:141\",\"byte\":15,\"long\":400000000000},\"long\":300000000000},\"long\":200000000000},\"long\":100000000000}";
      final String json = doc.toJSON();

      final ODocument loadedDoc =
          new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
      Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
      Assert.assertTrue(loadedDoc.field("long") instanceof Long);
      Assert.assertEquals(
          ((Long) doc.field("long")).longValue(), ((Long) loadedDoc.field("long")).longValue());
      Assert.assertTrue(loadedDoc.field("date") instanceof Date);
      Assert.assertTrue(loadedDoc.field("byte") instanceof Byte);
      Assert.assertEquals(
          ((Byte) doc.field("byte")).byteValue(), ((Byte) loadedDoc.field("byte")).byteValue());
      Assert.assertTrue(loadedDoc.field("doc") instanceof ODocument);

      final ODocument firstDoc = loadedDoc.field("doc");
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

      final ODocument secondDoc = firstDoc.field("doc");
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

      final ODocument thirdDoc = secondDoc.field("doc");
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
  public void testNestedEmbeddedMap() throws IOException {
    final ODocument newDoc = new ODocument();

    final Map<String, HashMap<?, ?>> map1 = new HashMap<>();
    newDoc.field("map1", map1, OType.EMBEDDEDMAP);

    final Map<String, HashMap<?, ?>> map2 = new HashMap<>();
    map1.put("map2", (HashMap<?, ?>) map2);

    final Map<String, HashMap<?, ?>> map3 = new HashMap<>();
    map2.put("map3", (HashMap<?, ?>) map3);

    String json = newDoc.toJSON();
    final ODocument loadedDoc =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));

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
  public void testFetchedJson() throws IOException {
    final OObjectDatabaseTx database = new OObjectDatabaseTx(url);
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

      final List<ODocument> result =
          database
              .getUnderlying()
              .command(
                  new OSQLSynchQuery<ODocument>(
                      "select * from Profile where name = 'Barack' and surname = 'Obama'"))
              .execute();

      for (final ODocument doc : result) {
        final String jsonFull =
            doc.toJSON("type,rid,version,class,keepTypes,attribSameRow,indent:0,fetchPlan:*:-1");
        final ODocument loadedDoc =
            new ODocument()
                .fromJSON(new ByteArrayInputStream(jsonFull.getBytes(StandardCharsets.UTF_8)));
        Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
      }
    } finally {
      database.close();
    }
  }

  // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
  public void testSpecialChar() throws IOException {
    final ODocument doc =
        new ODocument()
            .fromJSON(
                new ByteArrayInputStream(
                    "{name:{\"%Field\":[\"value1\",\"value2\"],\"%Field2\":{},\"%Field3\":\"value3\"}}"
                        .getBytes(StandardCharsets.UTF_8)));
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertEquals(doc, loadedDoc);
  }

  // Required loading @class
  public void testArrayOfArray() throws IOException {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        new ByteArrayInputStream(
            "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 100,  0 ],  [ 101, 1 ] ]}"
                .getBytes(StandardCharsets.UTF_8)));
    doc.save();
    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
  }

  // Required loading @class
  public void testLongTypes() throws IOException {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        new ByteArrayInputStream(
            "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 32874387347347,  0 ],  [ -23736753287327, 1 ] ]}"
                .getBytes(StandardCharsets.UTF_8)));
    doc.save();
    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertTrue(doc.hasSameContentOf(loadedDoc));
  }

  // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
  public void testSpecialChars() throws IOException {
    final ODocument doc =
        new ODocument()
            .fromJSON(
                new ByteArrayInputStream(
                    "{Field:{\"Key1\":[\"Value1\",\"Value2\"],\"Key2\":{\"%%dummy%%\":null},\"Key3\":\"Value3\"}}"
                        .getBytes(StandardCharsets.UTF_8)));
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ODocument loadedDoc = database.load(doc.getIdentity());
    Assert.assertEquals(doc, loadedDoc);
  }

  // TODO
  public void testJsonToStream() throws IOException {
    final String doc1Json =
        "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    final ODocument doc1 =
        new ODocument()
            .fromJSON(new ByteArrayInputStream(doc1Json.getBytes(StandardCharsets.UTF_8)));
    final String doc1String = new String(ORecordSerializerSchemaAware2CSV.INSTANCE.toStream(doc1));
    Assert.assertEquals(doc1Json, "{" + doc1String + "}");

    final String doc2Json =
        "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}";
    final ODocument doc2 =
        new ODocument()
            .fromJSON(new ByteArrayInputStream(doc2Json.getBytes(StandardCharsets.UTF_8)));
    final String doc2String = new String(ORecordSerializerSchemaAware2CSV.INSTANCE.toStream(doc2));
    Assert.assertEquals(doc2Json, "{" + doc2String + "}");
  }

  // "@fieldTypes":"out=z"
  /*  public void testSameNameCollectionsAndMap() throws IOException {
    ODocument doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    List<ODocument> list = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ODocument doc1 = new ODocument();
      doc.field("number", i);
      list.add(doc1);
      final Map<String, ODocument> docMap = new HashMap<>();
      for (int j = 0; j < 5; j++) {
        final ODocument doc2 = new ODocument();
        doc2.field("blabla", j);
        docMap.put(String.valueOf(j), doc2);
        final ODocument doc3 = new ODocument();
        doc3.field("blubli", String.valueOf(i + j));
        doc2.field("out", doc3);
      }
      doc1.field("out", docMap);
      list.add(doc1);
    }
    doc.field("out", list);

    // TODO: WIP move @fileTypes from end to signature:
    String json =
        "{\"@type\":\"d\",\"@version\":0,\"@fieldTypes\":\"out=z\",\"number\":9,\"string\":\"STRING_VALUE\",\"out\":[{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"0\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"0\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"13\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"13\"}}}}]}";
    // String json = doc.toJSON();

    ODocument newDoc =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(
        "{\"@type\":\"d\",\"@version\":0,\"number\":9,\"string\":\"STRING_VALUE\",\"out\":[{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"0\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"0\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"1\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"2\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"3\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"4\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"5\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"6\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"7\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"8\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"13\"}}}},{\"@type\":\"d\",\"@version\":0,\"out\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"9\"}},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"10\"}},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"11\"}},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"12\"}},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4,\"out\":{\"@type\":\"d\",\"@version\":0,\"blubli\":\"13\"}}}}],\"@fieldTypes\":\"out=z\"}",
        // json,
        newDoc.toJSON());
    Assert.assertTrue(newDoc.hasSameContentOf(doc));

    doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    final Map<String, ODocument> docMap = new HashMap<>();
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
    // FIXME: WIP move all @fileTypes from end to signatures:
    // json = doc.toJSON();

    // newDoc = new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes()));
    // Assert.assertEquals(json, newDoc.toJSON());
    // Assert.assertTrue(newDoc.hasSameContentOf(doc));
  }

  // "@fieldTypes":"out=z"
  public void testSameNameCollectionsAndMap2() throws IOException {
    final ODocument doc = new ODocument();
    doc.field("string", "STRING_VALUE");
    final List<ODocument> list = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      final ODocument doc1 = new ODocument();
      list.add(doc1);
      final Map<String, ODocument> docMap = new HashMap<>();
      for (int j = 0; j < 5; j++) {
        final ODocument doc2 = new ODocument();
        doc2.field("blabla", j);
        docMap.put(String.valueOf(j), doc2);
      }
      doc1.field("theMap", docMap);
      list.add(doc1);
    }
    doc.field("theList", list);
    // TODO: WIP move @fileTypes from end to signature:
    final String json =
        "{\"@type\":\"d\",\"@version\":0,\"@fieldTypes\":\"theList=z\",\"theList\":[{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}},{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}},{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}},{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}}],\"string\":\"STRING_VALUE\"}";
    // final String json = doc.toJSON();

    final ODocument newDoc =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(
        "{\"@type\":\"d\",\"@version\":0,\"theList\":[{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}},{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}},{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}},{\"@type\":\"d\",\"@version\":0,\"theMap\":{\"0\":{\"@type\":\"d\",\"@version\":0,\"blabla\":0},\"1\":{\"@type\":\"d\",\"@version\":0,\"blabla\":1},\"2\":{\"@type\":\"d\",\"@version\":0,\"blabla\":2},\"3\":{\"@type\":\"d\",\"@version\":0,\"blabla\":3},\"4\":{\"@type\":\"d\",\"@version\":0,\"blabla\":4}}}],\"string\":\"STRING_VALUE\",\"@fieldTypes\":\"theList=z\"}",
        // json,
        newDoc.toJSON());
    Assert.assertTrue(newDoc.hasSameContentOf(doc));
  }*/

  public void testSameNameCollectionsAndMap3() throws IOException {
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
    ODocument newDoc =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(newDoc.toJSON(), json);
  }

  public void testSpaces() throws IOException {
    final ODocument doc = new ODocument();
    final String test =
        "{"
            + "\"embedded\": {"
            + "\"second_embedded\":  {"
            + "\"text\":\"this is a test\""
            + "}"
            + "}"
            + "}";
    doc.fromJSON(new ByteArrayInputStream(test.getBytes(StandardCharsets.UTF_8)));
    Assert.assertTrue(doc.toJSON("fetchPlan:*:0,rid").indexOf("this is a test") > -1);
  }

  public void testEscaping() throws IOException {
    final ODocument doc = new ODocument();
    final String s =
        "{\"name\": \"test\", \"nested\": { \"key\": \"value\", \"anotherKey\": 123 }, \"deep\": {\"deeper\": { \"k\": \"v\",\"quotes\": \"\\\"\\\",\\\"oops\\\":\\\"123\\\"\", \"likeJson\": \"[1,2,3]\",\"spaces\": \"value with spaces\"}}}";
    doc.fromJSON(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("deep[deeper][quotes]"), "\"\",\"oops\":\"123\"");

    final String res = doc.toJSON();

    // LOOK FOR "quotes": \"\",\"oops\":\"123\"
    Assert.assertTrue(res.contains("\"quotes\":\"\\\"\\\",\\\"oops\\\":\\\"123\\\"\""));
  }

  public void testEscapingDoubleQuotes() throws IOException {
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
    doc.fromJSON(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("foo.three"), "a");
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"\"");
  }

  // Requires JsonParser.Feature.ALLOW_TRAILING_COMMA
  public void testEscapingDoubleQuotes2() throws IOException {
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

    doc.fromJSON(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("foo.three"), "a");
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"");
  }

  public void testEscapingDoubleQuotes3() throws IOException {
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

    doc.fromJSON(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
    final Collection c = doc.field("foo.bar.P357");
    Assert.assertEquals(c.size(), 1);
    final Map doc2 = (Map) c.iterator().next();
    Assert.assertEquals(((Map) doc2.get("datavalue")).get("value"), "\"");
  }

  public void testEmbeddedQuotes() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder builder = new StringBuilder();
    // FROM ISSUE 3151
    builder.append("{\"mainsnak\":{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}}");
    doc.fromJSON(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Sub\\urban");
  }

  public void testEmbeddedQuotes2() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}");
    doc.fromJSON(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("datavalue.value"), "Sub\\urban");
  }

  public void testEmbeddedQuotes2a() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":\"Sub\\\\urban\"}");
    doc.fromJSON(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("datavalue"), "Sub\\urban");
  }

  // TODO: fallback to legacy parser for invalid JSON
  /*public void testEmbeddedQuotes3() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}}");
    doc.fromJSON(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\\"");
  }

  // TODO: fallback to legacy parser for invalid JSON
  public void testEmbeddedQuotes4() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}");
    doc.fromJSON(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\\"");
  }

  // TODO: fallback to legacy parser for invalid JSON
  public void testEmbeddedQuotes5() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"datavalue\":\"Suburban\\\\\"\"}");
    doc.fromJSON(new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("datavalue"), "Suburban\\\"");
  }*/

  public void testEmbeddedQuotes6() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder builder = new StringBuilder();
    builder.append("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"}}}");
    doc.fromJSON(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\");
  }

  public void testEmbeddedQuotes7() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":{\"value\":\"Suburban\\\\\"}}");
    doc.fromJSON(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\");
  }

  public void testEmbeddedQuotes8() throws IOException {
    ODocument doc = new ODocument();
    StringBuilder builder = new StringBuilder();
    builder.append("{\"datavalue\":\"Suburban\\\\\"}");
    doc.fromJSON(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.field("datavalue"), "Suburban\\");
  }

  public void testEmpty() throws IOException {
    final ODocument doc = new ODocument();
    final StringBuilder builder = new StringBuilder();
    builder.append("{}");
    doc.fromJSON(new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(doc.fieldNames().length, 0);
  }

  public void testInvalidJson() {
    final ODocument doc = new ODocument();
    try {
      doc.fromJSON(new ByteArrayInputStream("{".getBytes(StandardCharsets.UTF_8)));
      Assert.fail();
    } catch (OSerializationException | IOException e) {
    }

    try {
      doc.fromJSON(new ByteArrayInputStream("{\"foo\":{}".getBytes(StandardCharsets.UTF_8)));
      Assert.fail();
    } catch (OSerializationException | IOException e) {
    }

    try {
      doc.fromJSON(new ByteArrayInputStream("{{}".getBytes(StandardCharsets.UTF_8)));
      Assert.fail();
    } catch (OSerializationException | IOException e) {
    }

    try {
      doc.fromJSON(new ByteArrayInputStream("{}}".getBytes(StandardCharsets.UTF_8)));
      Assert.fail();
    } catch (OSerializationException | IOException e) {
    }

    try {
      doc.fromJSON(new ByteArrayInputStream("}".getBytes(StandardCharsets.UTF_8)));
      Assert.fail();
    } catch (OSerializationException | IOException e) {
    }
  }

  // @fieldTypes parsing
  public void testDates() throws IOException {
    final Date now = new Date(1350518475000l);

    final ODocument doc = new ODocument();
    doc.field("date", now);
    // TODO: WIP move @fileTypes from end to signature: {"@type":"d","@version":0,"date":"2012-10-18
    // 02:01:15","@fieldTypes":"date=t"}
    // final String json =
    //    "{\"@type\":\"d\",\"@version\":0,\"@fieldTypes\":\"date=t\",\"date\":\"2012-10-18
    // 02:01:15\"}";
    final String json = doc.toJSON();

    final ODocument unmarshalled =
        new ODocument().fromJSON(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals(unmarshalled.field("date"), now);
  }

  @Test
  public void testList() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream("{\"list\" : [\"string\", 42]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), "string");
    Assert.assertEquals(list.get(1), 42);
  }

  @Test
  public void testEmbeddedRIDBagDeserialisationWhenFieldTypeIsProvided() throws Exception {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream(
            "{FirstName:\"Student A 0\",in_EHasGoodStudents:[#57:0],@fieldTypes:\"in_EHasGoodStudents=g\"}"
                .getBytes(StandardCharsets.UTF_8)));
    final ORidBag bag = documentSource.field("in_EHasGoodStudents");
    Assert.assertEquals(bag.size(), 1);
    final OIdentifiable rid = bag.rawIterator().next();
    Assert.assertTrue(rid.getIdentity().getClusterId() == 57);
    Assert.assertTrue(rid.getIdentity().getClusterPosition() == 0);
  }

  public void testNestedLinkCreation() throws IOException {
    ODocument jaimeDoc = new ODocument("NestedLinkCreation");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();

    // The link between jaime and cersei is saved properly - the #2263 test case
    ODocument cerseiDoc = new ODocument("NestedLinkCreation");
    final String jsonString =
        "{\"@type\":\"d\",\"name\":\"cersei\",\"valonqar\":" + jaimeDoc.toJSON() + "}";
    cerseiDoc.fromJSON(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
    cerseiDoc.save();

    // The link between jamie and tyrion is not saved properly
    final ODocument tyrionDoc = new ODocument("NestedLinkCreation");
    final String jsonString2 =
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\", \"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.toJSON()
            + "}}";
    tyrionDoc.fromJSON(new ByteArrayInputStream(jsonString2.getBytes(StandardCharsets.UTF_8)));
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

  // TODO: fallback to legacy parser for invalid JSON
  /*public void testNestedLinkCreationFieldTypes() throws IOException {
    ODocument jaimeDoc = new ODocument("NestedLinkCreationFieldTypes");
    jaimeDoc.field("name", "jaime");
    jaimeDoc.save();

    // The link between jaime and cersei is saved properly - the #2263 test case
    ODocument cerseiDoc = new ODocument("NestedLinkCreationFieldTypes");
    String jsonString =
        "{\"@type\":\"d\",\"@fieldTypes\":\"valonqar=x\",\"name\":\"cersei\",\"valonqar\":"
            + jaimeDoc.getIdentity()
            + "}";
    cerseiDoc.fromJSON(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
    cerseiDoc.save();

    // The link between jamie and tyrion is not saved properly
    ODocument tyrionDoc = new ODocument("NestedLinkCreationFieldTypes");
    jsonString =
        "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\", \"@fieldTypes\":\"contact=x\",\"relationship\":\"brother\",\"contact\":"
            + jaimeDoc.getIdentity()
            + "}}";
    tyrionDoc.fromJSON(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
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
  }*/

  public void testInnerDocCreation() throws IOException {
    ODocument adamDoc = new ODocument("InnerDocCreation");
    adamDoc.fromJSON(
        new ByteArrayInputStream("{\"name\":\"adam\"}".getBytes(StandardCharsets.UTF_8)));
    adamDoc.save();

    ODocument eveDoc = new ODocument("InnerDocCreation");
    final String jsonString =
        "{\"@type\":\"d\",\"name\":\"eve\",\"friends\":[" + adamDoc.toJSON() + "]}";
    eveDoc.fromJSON(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
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

  // TODO: fallback to legacy parser for invalid JSON
  /*public void testInnerDocCreationFieldTypes() throws IOException {
    final ODocument adamDoc = new ODocument("InnerDocCreationFieldTypes");
    adamDoc.fromJSON(new ByteArrayInputStream("{\"name\":\"adam\"}".getBytes(StandardCharsets.UTF_8)));
    adamDoc.save();

    final ODocument eveDoc = new ODocument("InnerDocCreationFieldTypes");
    final String jsonString =
        "{\"@type\":\"d\", \"@fieldTypes\" : \"friends=z\", \"name\":\"eve\",\"friends\":["
            + adamDoc.getIdentity()
            + "]}";
    eveDoc.fromJSON(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
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
  }*/

  public void testJSONTxDocInsertOnly() throws IOException {
    final String classNameOne = "JSONTxDocOneInsertOnly";
    if (!database.getMetadata().getSchema().existsClass(classNameOne)) {
      database.getMetadata().getSchema().createClass(classNameOne);
    }
    final String classNameTwo = "JSONTxDocTwoInsertOnly";
    if (!database.getMetadata().getSchema().existsClass(classNameTwo)) {
      database.getMetadata().getSchema().createClass(classNameTwo);
    }
    database.begin();
    final ODocument eveDoc = new ODocument(classNameOne);
    eveDoc.field("name", "eve");
    eveDoc.save();

    final ODocument nestedWithTypeD = new ODocument(classNameTwo);
    final String jsonString =
        "{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":[" + eveDoc.toJSON() + "]}";
    nestedWithTypeD.fromJSON(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
    nestedWithTypeD.save();
    database.commit();
    Assert.assertEquals(database.countClass(classNameOne), 1);

    final Map<ORID, ODocument> contentMap = new HashMap<>();

    final ODocument eve = new ODocument(classNameOne);
    eve.field("name", "eve");
    contentMap.put(eveDoc.getIdentity(), eve);

    for (final ODocument document : database.browseClass(classNameOne)) {
      final ODocument content = contentMap.get(document.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(document));
    }
  }

  public void testJSONTxDoc() throws IOException {
    if (!database.getMetadata().getSchema().existsClass("JSONTxDocOne")) {
      database.getMetadata().getSchema().createClass("JSONTxDocOne");
    }

    if (!database.getMetadata().getSchema().existsClass("JSONTxDocTwo")) {
      database.getMetadata().getSchema().createClass("JSONTxDocTwo");
    }
    final ODocument adamDoc = new ODocument("JSONTxDocOne");
    adamDoc.field("name", "adam");
    adamDoc.save();

    database.begin();
    final ODocument eveDoc = new ODocument("JSONTxDocOne");
    eveDoc.field("name", "eve");
    eveDoc.save();

    final ODocument nestedWithTypeD = new ODocument("JSONTxDocTwo");
    final String jsonString =
        "{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":["
            + eveDoc.toJSON()
            + ","
            + adamDoc.toJSON()
            + "]}";
    nestedWithTypeD.fromJSON(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
    nestedWithTypeD.save();
    database.commit();
    Assert.assertEquals(database.countClass("JSONTxDocOne"), 2);

    final Map<ORID, ODocument> contentMap = new HashMap<>();
    final ODocument adam = new ODocument("JSONTxDocOne");
    adam.field("name", "adam");
    contentMap.put(adamDoc.getIdentity(), adam);

    final ODocument eve = new ODocument("JSONTxDocOne");
    eve.field("name", "eve");
    contentMap.put(eveDoc.getIdentity(), eve);

    for (final ODocument o : database.browseClass("JSONTxDocOne")) {
      final ODocument content = contentMap.get(o.getIdentity());
      Assert.assertTrue(content.hasSameContentOf(o));
    }
  }

  public void testInvalidLink() throws IOException {
    final ODocument nullRefDoc = new ODocument();
    nullRefDoc.fromJSON(
        new ByteArrayInputStream(
            "{\"name\":\"Luca\", \"ref\":\"#-1:-1\"}".getBytes(StandardCharsets.UTF_8)));
    // Assert.assertNull(nullRefDoc.rawField("ref"));

    final String json = nullRefDoc.toJSON();
    int pos = json.indexOf("\"ref\":");

    Assert.assertTrue(pos > -1);
    Assert.assertEquals(json.charAt(pos + "\"ref\":".length()), 'n');
  }

  public void testOtherJson() throws IOException {
    new ODocument()
        .fromJSON(
            new ByteArrayInputStream(
                "{\"Salary\":1500.0,\"Type\":\"Person\",\"Address\":[{\"Zip\":\"JX2 MSX\",\"Type\":\"Home\",\"Street1\":\"13 Marge Street\",\"Country\":\"Holland\",\"Id\":\"Address-28813211\",\"City\":\"Amsterdam\",\"From\":\"1996-02-01\",\"To\":\"1998-01-01\"},{\"Zip\":\"90210\",\"Type\":\"Work\",\"Street1\":\"100 Hollywood Drive\",\"Country\":\"USA\",\"Id\":\"Address-11595040\",\"City\":\"Los Angeles\",\"From\":\"2009-09-01\"}],\"Id\":\"Person-7464251\",\"Name\":\"Stan\"}"
                    .getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testNumericFloatListScientific() throws IOException {
    final ODocument documentSource = new ODocument();
    documentSource.fromJSON(
        new ByteArrayInputStream(
            "{\"list\" : [-9.27415E-31,741800E+290]}".getBytes(StandardCharsets.UTF_8)));

    final ODocument documentTarget = new ODocument();
    documentTarget.fromStream(documentSource.toStream());

    final OTrackedList<Object> list = documentTarget.field("list", OType.EMBEDDEDLIST);
    Assert.assertEquals(list.get(0), -9.27415E-31);
    Assert.assertEquals(list.get(1), 741800E+290);
  }

  @Test
  public void testScientificNotation() throws IOException {
    final ODocument doc = new ODocument();
    doc.fromJSON(
        new ByteArrayInputStream(
            "{'number1': -9.2741500e-31, 'number2': 741800E+290}"
                .getBytes(StandardCharsets.UTF_8)));

    final double number1 = doc.field("number1");
    Assert.assertEquals(number1, -9.27415E-31);
    final double number2 = doc.field("number2");
    Assert.assertEquals(number2, 741800E+290);
  }
}
