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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be
 * because the order of clusters could be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-update")
public class SQLUpdateTest extends DocumentDBBaseTest {
  private long updatedRecords;
  private int addressClusterId;

  @Parameters(value = "url")
  public SQLUpdateTest(@Optional String iURL) {
    super(iURL);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    OClass addressClass = database.getMetadata().getSchema().getClass("Address");
    if (addressClass == null) {
      addressClass = database.getMetadata().getSchema().createClass("Address");
    }

    addressClusterId = addressClass.getDefaultClusterId();
  }

  @Test
  public void updateWithWhereOperator() {

    List<Long> positions = getValidPositions(addressClusterId);

    OResultSet records =
        database.command(
            "update Profile set salary = 120.30, location = "
                + addressClusterId
                + ":"
                + positions.get(2)
                + ", salary_cloned = salary where surname = 'Obama'");

    Assert.assertEquals(((Number) records.next().getProperty("count")).intValue(), 3);
  }

  @Test
  public void updateWithWhereRid() {

    List<OResult> result =
        database.command("select @rid as rid from Profile where surname = 'Obama'").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 3);

    OResultSet records =
        database.command(
            "update Profile set salary = 133.00 where @rid = ?",
            result.get(0).<Object>getProperty("rid"));

    Assert.assertEquals(((Number) records.next().getProperty("count")).intValue(), 1);
  }

  @Test
  public void updateUpsertOperator() {

    OResultSet result =
        database.command(
            "UPDATE Profile SET surname='Merkel' RETURN AFTER where surname = 'Merkel'");
    Assert.assertEquals(result.stream().count(), 0);

    result =
        database.command(
            "UPDATE Profile SET surname='Merkel' UPSERT RETURN AFTER  where surname = 'Merkel'");
    Assert.assertEquals(result.stream().count(), 1);

    result = database.command("SELECT FROM Profile  where surname = 'Merkel'");
    Assert.assertEquals(result.stream().count(), 1);
  }

  @Test(dependsOnMethods = "updateWithWhereOperator")
  public void updateCollectionsAddWithWhereOperator() {
    updatedRecords =
        database
            .command("update Account set addresses = addresses || #" + addressClusterId + ":0")
            .next()
            .getProperty("count");
  }

  @Test(dependsOnMethods = "updateCollectionsAddWithWhereOperator")
  public void updateCollectionsRemoveWithWhereOperator() {

    final long records =
        database
            .command("update Account remove addresses = #" + addressClusterId + ":0")
            .next()
            .getProperty("count");

    Assert.assertEquals(records, updatedRecords);
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateCollectionsWithSetOperator() {

    List<OResult> docs =
        database.query("select from Account").stream().collect(Collectors.toList());

    List<Long> positions = getValidPositions(addressClusterId);

    for (OResult doc : docs) {

      final long records =
          database
              .command(
                  "update Account set addresses = [#"
                      + addressClusterId
                      + ":"
                      + positions.get(0)
                      + ", #"
                      + addressClusterId
                      + ":"
                      + positions.get(1)
                      + ",#"
                      + addressClusterId
                      + ":"
                      + positions.get(2)
                      + "] where @rid = "
                      + doc.getIdentity().get())
              .next()
              .getProperty("count");

      Assert.assertEquals(records, 1);

      ODocument loadedDoc = database.load(doc.getIdentity().get(), "*:-1", true);
      Assert.assertEquals(((List<?>) loadedDoc.field("addresses")).size(), 3);
      Assert.assertEquals(
          ((OIdentifiable) ((List<?>) loadedDoc.field("addresses")).get(0))
              .getIdentity()
              .toString(),
          "#" + addressClusterId + ":" + positions.get(0));
      loadedDoc.field("addresses", doc.<Object>getProperty("addresses"));
      database.save(loadedDoc);
    }
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateMapsWithSetOperator() {

    ODocument doc =
        database
            .command(
                new OCommandSQL(
                    "insert into cluster:default (equaledges, name, properties) values ('no', 'circleUpdate', {'round':'eeee', 'blaaa':'zigzag'} )"))
            .execute();

    Integer records =
        database
            .command(
                new OCommandSQL(
                    "update "
                        + doc.getIdentity()
                        + " set properties = {'roundOne':'ffff', 'bla':'zagzig','testTestTEST':'okOkOK'}"))
            .execute();

    Assert.assertEquals(records.intValue(), 1);

    ODocument loadedDoc = database.load(doc.getIdentity(), "*:-1", true);

    Assert.assertTrue(loadedDoc.field("properties") instanceof Map);

    @SuppressWarnings("unchecked")
    Map<Object, Object> entries = loadedDoc.field("properties");
    Assert.assertEquals(entries.size(), 3);

    Assert.assertNull(entries.get("round"));
    Assert.assertNull(entries.get("blaaa"));

    Assert.assertEquals(entries.get("roundOne"), "ffff");
    Assert.assertEquals(entries.get("bla"), "zagzig");
    Assert.assertEquals(entries.get("testTestTEST"), "okOkOK");
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateMapsWithPutOperatorAndWhere() {

    ODocument doc =
        database
            .command(
                new OCommandSQL(
                    "insert into cluster:default (equaledges, name, properties) values ('no', 'updateMapsWithPutOperatorAndWhere', {} )"))
            .execute();

    Integer records =
        database
            .command(
                new OCommandSQL(
                    "update "
                        + doc.getIdentity()
                        + " put properties = 'one', 'two' where name = 'updateMapsWithPutOperatorAndWhere'"))
            .execute();

    Assert.assertEquals(records.intValue(), 1);

    ODocument loadedDoc = database.load(doc.getIdentity(), "*:-1", true);

    Assert.assertTrue(loadedDoc.field("properties") instanceof Map);

    @SuppressWarnings("unchecked")
    Map<Object, Object> entries = loadedDoc.field("properties");
    Assert.assertEquals(entries.size(), 1);

    Assert.assertNull(entries.get("round"));
    Assert.assertNull(entries.get("blaaa"));

    Assert.assertEquals(entries.get("one"), "two");
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateAllOperator() {

    Long total = database.countClass("Profile");

    Long records = database.command("update Profile set sex = 'male'").next().getProperty("count");

    Assert.assertEquals(records.intValue(), total.intValue());
  }

  @Test(dependsOnMethods = "updateAllOperator")
  public void updateWithWildcards() {

    long updated =
        database
            .command("update Profile set sex = ? where sex = 'male' limit 1", "male")
            .next()
            .getProperty("count");

    Assert.assertEquals(updated, 1);
  }

  @Test
  public void updateWithWildcardsOnSetAndWhere() {

    ODocument doc = new ODocument("Person");
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");
    doc.save();
    checkUpdatedDoc(database, "Raf", "Torino", "fmale");

    /* THESE COMMANDS ARE OK */
    database.command("update Person set gender = 'female' where name = 'Raf'", "Raf");
    checkUpdatedDoc(database, "Raf", "Torino", "female");

    database.command("update Person set city = 'Turin' where name = ?", "Raf");
    checkUpdatedDoc(database, "Raf", "Turin", "female");

    database.command("update Person set gender = ? where name = 'Raf'", "F");
    checkUpdatedDoc(database, "Raf", "Turin", "F");

    database.command(
        "update Person set gender = ?, city = ? where name = 'Raf'", "FEMALE", "TORINO");
    checkUpdatedDoc(database, "Raf", "TORINO", "FEMALE");

    database.command("update Person set gender = ? where name = ?", "f", "Raf");
    checkUpdatedDoc(database, "Raf", "TORINO", "f");
  }

  public void updateWithReturn() {
    ODocument doc = new ODocument("Data");
    doc.field("name", "Pawel");
    doc.field("city", "Wroclaw");
    doc.field("really_big_field", "BIIIIIIIIIIIIIIIGGGGGGG!!!");
    doc.save();
    // check AFTER
    String sqlString = "UPDATE " + doc.getIdentity().toString() + " SET gender='male' RETURN AFTER";
    List<ODocument> result1 = database.command(new OCommandSQL(sqlString)).execute();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertEquals(result1.get(0).getIdentity(), doc.getIdentity());
    Assert.assertEquals((String) result1.get(0).field("gender"), "male");
    final ODocument lastOne = result1.get(0).copy();
    // check record attributes and BEFORE
    sqlString = "UPDATE " + doc.getIdentity().toString() + " SET Age=1 RETURN BEFORE @this";
    result1 = database.command(new OCommandSQL(sqlString)).execute();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertEquals(lastOne.getVersion(), result1.get(0).getVersion());
    Assert.assertFalse(result1.get(0).containsField("Age"));
    // check INCREMENT, AFTER + $current + field
    sqlString =
        "UPDATE " + doc.getIdentity().toString() + " INCREMENT Age = 100 RETURN AFTER $current.Age";
    result1 = database.command(new OCommandSQL(sqlString)).execute();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertTrue(result1.get(0).containsField("value"));
    Assert.assertEquals(result1.get(0).<Object>field("value"), 101);
    // check exclude + WHERE + LIMIT
    sqlString =
        "UPDATE "
            + doc.getIdentity().toString()
            + " INCREMENT Age = 100 RETURN AFTER $current.Exclude('really_big_field') WHERE Age=101 LIMIT 1";
    result1 = database.command(new OCommandSQL(sqlString)).execute();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertTrue(result1.get(0).containsField("Age"));
    Assert.assertEquals(result1.get(0).<Object>field("Age"), 201);
    Assert.assertFalse(result1.get(0).containsField("really_big_field"));
  }

  @Test
  public void updateWithNamedParameters() {
    ODocument doc = new ODocument("Data");
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");
    doc.save();

    String updatecommand = "update Data set gender = :gender , city = :city where name = :name";
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("gender", "f");
    params.put("city", "TOR");
    params.put("name", "Raf");

    database.command(updatecommand, params);
    OResultSet result = database.query("select * from Data");
    OResult oDoc = result.next();
    Assert.assertEquals("Raf", oDoc.getProperty("name"));
    Assert.assertEquals("TOR", oDoc.getProperty("city"));
    Assert.assertEquals("f", oDoc.getProperty("gender"));
    result.close();
  }

  public void updateIncrement() {

    List<OResult> result1 =
        database.query("select salary from Account where salary is defined").stream()
            .collect(Collectors.toList());
    Assert.assertFalse(result1.isEmpty());

    updatedRecords =
        database
            .command("update Account set salary += 10 where salary is defined")
            .next()
            .getProperty("count");
    Assert.assertTrue(updatedRecords > 0);

    List<OResult> result2 =
        database.query("select salary from Account where salary is defined").stream()
            .collect(Collectors.toList());
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary2 = result2.get(i).getProperty("salary");
      Assert.assertEquals(salary2, salary1 + 10);
    }

    updatedRecords =
        database
            .command("update Account set salary -= 10 where salary is defined")
            .next()
            .getProperty("count");
    Assert.assertTrue(updatedRecords > 0);

    List<OResult> result3 =
        database.command("select salary from Account where salary is defined").stream()
            .collect(Collectors.toList());
    Assert.assertFalse(result3.isEmpty());
    Assert.assertEquals(result3.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary3 = result3.get(i).getProperty("salary");
      Assert.assertEquals(salary3, salary1);
    }
  }

  public void updateSetMultipleFields() {

    List<OResult> result1 =
        database.query("select salary from Account where salary is defined").stream()
            .collect(Collectors.toList());
    Assert.assertFalse(result1.isEmpty());

    updatedRecords =
        database
            .command(
                "update Account set salary2 = salary, checkpoint = true where salary is defined")
            .next()
            .getProperty("count");
    Assert.assertTrue(updatedRecords > 0);

    List<OResult> result2 =
        database.query("select from Account where salary is defined").stream()
            .collect(Collectors.toList());
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).getProperty("salary");
      float salary2 = result2.get(i).getProperty("salary2");
      Assert.assertEquals(salary2, salary1);
      Assert.assertEquals(result2.get(i).<Object>getProperty("checkpoint"), true);
    }
  }

  public void updateAddMultipleFields() {

    updatedRecords =
        database
            .command("update Account set myCollection = myCollection || [1,2] limit 1")
            .next()
            .getProperty("count");
    Assert.assertTrue(updatedRecords > 0);

    List<OResult> result2 =
        database.command("select from Account where myCollection is defined").stream()
            .collect(Collectors.toList());
    Assert.assertEquals(result2.size(), 1);

    Collection<Object> myCollection = result2.iterator().next().getProperty("myCollection");

    Assert.assertTrue(myCollection.containsAll(Arrays.asList(1, 2)));
  }

  public void testEscaping() {
    final OSchema schema = database.getMetadata().getSchema();
    schema.createClass("FormatEscapingTest");

    final ODocument document = new ODocument("FormatEscapingTest");
    document.save();

    database
        .command(
            "UPDATE FormatEscapingTest SET test = format('aaa \\' bbb') WHERE @rid = "
                + document.getIdentity())
        .close();

    document.reload();

    Assert.assertEquals(document.field("test"), "aaa ' bbb");

    database
        .command(
            "UPDATE FormatEscapingTest SET test = 'ccc \\' eee', test2 = format('aaa \\' bbb') WHERE @rid = "
                + document.getIdentity())
        .close();

    document.reload();
    Assert.assertEquals(document.field("test"), "ccc ' eee");
    Assert.assertEquals(document.field("test2"), "aaa ' bbb");

    database
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\n bbb' WHERE @rid = "
                + document.getIdentity())
        .close();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \n bbb");

    database
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\r bbb' WHERE @rid = "
                + document.getIdentity())
        .close();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \r bbb");

    database
        .command(
            new OCommandSQL(
                "UPDATE FormatEscapingTest SET test = 'aaa \\b bbb' WHERE @rid = "
                    + document.getIdentity()))
        .execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \b bbb");

    database
        .command(
            "UPDATE FormatEscapingTest SET test = 'aaa \\t bbb' WHERE @rid = "
                + document.getIdentity())
        .close();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \t bbb");

    database
        .command(
            new OCommandSQL(
                "UPDATE FormatEscapingTest SET test = 'aaa \\f bbb' WHERE @rid = "
                    + document.getIdentity()))
        .execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \f bbb");
  }

  public void testUpdateVertexContent() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass vertex = schema.getClass("V");
    schema.createClass("UpdateVertexContent", vertex);

    final ORID vOneId =
        database.command("create vertex UpdateVertexContent").next().getIdentity().get();
    final ORID vTwoId =
        database.command("create vertex UpdateVertexContent").next().getIdentity().get();

    database.command("create edge from " + vOneId + " to " + vTwoId).close();
    database.command("create edge from " + vOneId + " to " + vTwoId).close();
    database.command("create edge from " + vOneId + " to " + vTwoId).close();

    List<OResult> result =
        database.query("select sum(outE().size(), inE().size()) as sum from UpdateVertexContent")
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);

    for (OResult doc : result) {
      Assert.assertEquals(doc.<Object>getProperty("sum"), 3);
    }

    database
        .command("update UpdateVertexContent content {value : 'val'} where @rid = " + vOneId)
        .close();
    database
        .command("update UpdateVertexContent content {value : 'val'} where @rid =  " + vTwoId)
        .close();

    result =
        database.query("select sum(outE().size(), inE().size()) as sum from UpdateVertexContent")
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);

    for (OResult doc : result) {
      Assert.assertEquals(doc.<Object>getProperty("sum"), 3);
    }

    result =
        database.query("select from UpdateVertexContent").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    for (OResult doc : result) {
      Assert.assertEquals(doc.getProperty("value"), "val");
    }
  }

  public void testUpdateEdgeContent() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass vertex = schema.getClass("V");
    OClass edge = schema.getClass("E");

    schema.createClass("UpdateEdgeContentV", vertex);
    schema.createClass("UpdateEdgeContentE", edge);

    final ORID vOneId =
        database.command("create vertex UpdateEdgeContentV").next().getIdentity().get();
    final ORID vTwoId =
        database.command("create vertex UpdateEdgeContentV").next().getIdentity().get();

    database.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    database.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();
    database.command("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId).close();

    List<OResult> result =
        database.query("select outV() as outV, inV() as inV from UpdateEdgeContentE").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 3);

    for (OResult doc : result) {
      Assert.assertEquals(doc.getProperty("outV"), vOneId);
      Assert.assertEquals(doc.getProperty("inV"), vTwoId);
    }

    database.command("update UpdateEdgeContentE content {value : 'val'}").close();

    result =
        database.query("select outV() as outV, inV() as inV from UpdateEdgeContentE").stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 3);

    for (OResult doc : result) {
      Assert.assertEquals(doc.getProperty("outV"), vOneId);
      Assert.assertEquals(doc.getProperty("inV"), vTwoId);
    }

    result = database.query("select from UpdateEdgeContentE").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 3);
    for (OResult doc : result) {
      Assert.assertEquals(doc.getProperty("value"), "val");
    }
  }

  private void checkUpdatedDoc(
      ODatabaseDocument database, String expectedName, String expectedCity, String expectedGender) {
    OResultSet result = database.query("select * from person");
    OResult oDoc = result.next();
    Assert.assertEquals(expectedName, oDoc.getProperty("name"));
    Assert.assertEquals(expectedCity, oDoc.getProperty("city"));
    Assert.assertEquals(expectedGender, oDoc.getProperty("gender"));
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final ORecordIteratorCluster<ODocument> iteratorCluster =
        database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 7; i++) {
      if (!iteratorCluster.hasNext()) break;
      ODocument doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }

  public void testMultiplePut() {
    final ODocument v = database.<ODocument>newInstance("V").save();

    Long records =
        database
            .command(
                "UPDATE"
                    + v.getIdentity()
                    + " SET embmap[\"test\"] = \"Luca\" ,embmap[\"test2\"]=\"Alex\"")
            .next()
            .getProperty("count");

    Assert.assertEquals(records.intValue(), 1);

    v.reload();

    Assert.assertTrue(v.field("embmap") instanceof Map);
    Assert.assertEquals(((Map) v.field("embmap")).size(), 2);
  }

  public void testAutoConversionOfEmbeddededListWithLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    if (!c.existsProperty("embeddedListWithLinkedClass"))
      c.createProperty(
          "embeddedListWithLinkedClass",
          OType.EMBEDDEDLIST,
          database.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    ORID id =
        database
            .command(
                "INSERT INTO TestConvert SET name = 'embeddedListWithLinkedClass', embeddedListWithLinkedClass = [{'line1':'123 Fake Street'}]")
            .next()
            .getIdentity()
            .get();

    database
        .command(
            "UPDATE "
                + id.getIdentity()
                + " set embeddedListWithLinkedClass = embeddedListWithLinkedClass || [{'line1':'123 Fake Street'}]")
        .close();

    OElement doc = database.reload(database.load(id), "", true);

    Assert.assertTrue(doc.getProperty("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.getProperty("embeddedListWithLinkedClass")).size(), 2);

    database
        .command(
            "UPDATE "
                + doc.getIdentity()
                + " set embeddedListWithLinkedClass =  embeddedListWithLinkedClass || [{'line1':'123 Fake Street'}]")
        .close();

    doc.reload();

    Assert.assertTrue(doc.getProperty("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.getProperty("embeddedListWithLinkedClass")).size(), 3);

    List addr = doc.getProperty("embeddedListWithLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof ODocument);
      Assert.assertEquals(((ODocument) o).getClassName(), "TestConvertLinkedClass");
    }
  }

  public void testPutListOfMaps() {
    String className = "testPutListOfMaps";
    database.getMetadata().getSchema().createClass(className);

    database
        .command("insert into " + className + " set list = [{\"xxx\":1},{\"zzz\":3},{\"yyy\":2}]")
        .close();
    database.command("UPDATE " + className + " set list = list || [{\"kkk\":4}]").close();

    List<OResult> result =
        database.query("select from " + className).stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 1);
    OResult doc = result.get(0);
    List list = doc.getProperty("list");
    Assert.assertEquals(list.size(), 4);
    Object fourth = list.get(3);

    Assert.assertTrue(fourth instanceof Map);
    Assert.assertEquals(((Map) fourth).keySet().iterator().next(), "kkk");
    Assert.assertEquals(((Map) fourth).values().iterator().next(), 4);
  }
}
