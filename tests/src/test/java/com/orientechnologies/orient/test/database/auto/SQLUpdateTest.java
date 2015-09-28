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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be because the order of clusters could
 * be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-update", sequential = true)
public class SQLUpdateTest extends DocumentDBBaseTest {
  private int updatedRecords;
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

    List<Long> positions = getValidPositions(4);

    Integer records = database.command(
        new OCommandSQL("update Profile set salary = 120.30, location = 4:" + positions.get(2)
            + ", salary_cloned = salary where surname = 'Obama'")).execute();

    Assert.assertEquals(records.intValue(), 3);

  }

  @Test
  public void updateWithWhereRid() {

    List<ODocument> result = database.command(new OCommandSQL("select @rid as rid from Profile where surname = 'Obama'")).execute();

    Assert.assertEquals(result.size(), 3);

    Integer records = database.command(new OCommandSQL("update Profile set salary = 133.00 where @rid = ?")).execute(
        result.get(0).field("rid"));

    Assert.assertEquals(records.intValue(), 1);

  }

  @Test
  public void updateUpsertOperator() {

    List<ODocument> result = database.command(
        new OCommandSQL("UPDATE Profile SET surname='Merkel' RETURN AFTER where surname = 'Merkel'")).execute();
    Assert.assertEquals(result.size(), 0);

    result = database.command(new OCommandSQL("UPDATE Profile SET surname='Merkel' UPSERT RETURN AFTER  where surname = 'Merkel'"))
        .execute();
    Assert.assertEquals(result.size(), 1);

    result = database.command(new OCommandSQL("SELECT FROM Profile  where surname = 'Merkel'")).execute();
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "updateWithWhereOperator")
  public void updateCollectionsAddWithWhereOperator() {
    updatedRecords = database.command(new OCommandSQL("update Account add addresses = #" + addressClusterId + ":0")).execute();
  }

  @Test(dependsOnMethods = "updateCollectionsAddWithWhereOperator")
  public void updateCollectionsRemoveWithWhereOperator() {

    final int records = database.command(new OCommandSQL("update Account remove addresses = #" + addressClusterId + ":0"))
        .execute();

    Assert.assertEquals(records, updatedRecords);
  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateCollectionsWithSetOperator() {

    List<ODocument> docs = database.query(new OSQLSynchQuery<ODocument>("select from Account"));

    List<Long> positions = getValidPositions(addressClusterId);

    for (ODocument doc : docs) {

      final int records = database.command(
          new OCommandSQL("update Account set addresses = [#" + addressClusterId + ":" + positions.get(0) + ", #"
              + addressClusterId + ":" + positions.get(1) + ",#" + addressClusterId + ":" + positions.get(2) + "] where @rid = "
              + doc.getIdentity())).execute();

      Assert.assertEquals(records, 1);

      ODocument loadedDoc = database.load(doc.getIdentity(), "*:-1", true);
      Assert.assertEquals(((List<?>) loadedDoc.field("addresses")).size(), 3);
      Assert.assertEquals(((OIdentifiable) ((List<?>) loadedDoc.field("addresses")).get(0)).getIdentity().toString(), "#"
          + addressClusterId + ":" + positions.get(0));
      loadedDoc.field("addresses", doc.field("addresses"));
      database.save(loadedDoc);
    }

  }

  @Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
  public void updateMapsWithSetOperator() {

    ODocument doc = database
        .command(
            new OCommandSQL(
                "insert into cluster:default (equaledges, name, properties) values ('no', 'circleUpdate', {'round':'eeee', 'blaaa':'zigzag'} )"))
        .execute();

    Integer records = database.command(
        new OCommandSQL("update " + doc.getIdentity()
            + " set properties = {'roundOne':'ffff', 'bla':'zagzig','testTestTEST':'okOkOK'}")).execute();

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

    ODocument doc = database.command(
        new OCommandSQL(
            "insert into cluster:default (equaledges, name, properties) values ('no', 'updateMapsWithPutOperatorAndWhere', {} )"))
        .execute();

    Integer records = database.command(
        new OCommandSQL("update " + doc.getIdentity()
            + " put properties = 'one', 'two' where name = 'updateMapsWithPutOperatorAndWhere'")).execute();

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

    Integer records = database.command(new OCommandSQL("update Profile set sex = 'male'")).execute();

    Assert.assertEquals(records.intValue(), total.intValue());

  }

  @Test(dependsOnMethods = "updateAllOperator")
  public void updateWithWildcards() {

    int updated = database.command(new OCommandSQL("update Profile set sex = ? where sex = 'male' limit 1")).execute("male");

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
    OCommandSQL updatecommand = new OCommandSQL("update Person set gender = 'female' where name = 'Raf'");
    database.command(updatecommand).execute("Raf");
    checkUpdatedDoc(database, "Raf", "Torino", "female");

    updatecommand = new OCommandSQL("update Person set city = 'Turin' where name = ?");
    database.command(updatecommand).execute("Raf");
    checkUpdatedDoc(database, "Raf", "Turin", "female");

    updatecommand = new OCommandSQL("update Person set gender = ? where name = 'Raf'");
    database.command(updatecommand).execute("F");
    checkUpdatedDoc(database, "Raf", "Turin", "F");

    updatecommand = new OCommandSQL("update Person set gender = ?, city = ? where name = 'Raf'");
    database.command(updatecommand).execute("FEMALE", "TORINO");
    checkUpdatedDoc(database, "Raf", "TORINO", "FEMALE");

    updatecommand = new OCommandSQL("update Person set gender = ? where name = ?");
    database.command(updatecommand).execute("f", "Raf");
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
    sqlString = "UPDATE " + doc.getIdentity().toString() + " INCREMENT Age = 100 RETURN AFTER $current.Age";
    result1 = database.command(new OCommandSQL(sqlString)).execute();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertTrue(result1.get(0).containsField("value"));
    Assert.assertEquals(result1.get(0).field("value"), 101);
    // check exclude + WHERE + LIMIT
    sqlString = "UPDATE " + doc.getIdentity().toString()
        + " INCREMENT Age = 100 RETURN AFTER $current.Exclude('really_big_field') WHERE Age=101 LIMIT 1";
    result1 = database.command(new OCommandSQL(sqlString)).execute();
    Assert.assertEquals(result1.size(), 1);
    Assert.assertTrue(result1.get(0).containsField("Age"));
    Assert.assertEquals(result1.get(0).field("Age"), 201);
    Assert.assertFalse(result1.get(0).containsField("really_big_field"));

  }

  @Test
  public void updateWithNamedParameters() {
    ODocument doc = new ODocument("Data");
    doc.field("name", "Raf");
    doc.field("city", "Torino");
    doc.field("gender", "fmale");
    doc.save();

    OCommandSQL updatecommand = new OCommandSQL("update Data set gender = :gender , city = :city where name = :name");
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("gender", "f");
    params.put("city", "TOR");
    params.put("name", "Raf");

    database.command(updatecommand).execute(params);
    List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select * from Data"));
    ODocument oDoc = result.get(0);
    Assert.assertEquals("Raf", oDoc.field("name"));
    Assert.assertEquals("TOR", oDoc.field("city"));
    Assert.assertEquals("f", oDoc.field("gender"));
  }

  public void updateIncrement() {

    List<ODocument> result1 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result1.isEmpty());

    updatedRecords = database.command(new OCommandSQL("update Account increment salary = 10 where salary is defined")).execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result2 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).field("salary");
      float salary2 = result2.get(i).field("salary");
      Assert.assertEquals(salary2, salary1 + 10);
    }

    updatedRecords = database.command(new OCommandSQL("update Account increment salary = -10 where salary is defined")).execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result3 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result3.isEmpty());
    Assert.assertEquals(result3.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).field("salary");
      float salary3 = result3.get(i).field("salary");
      Assert.assertEquals(salary3, salary1);
    }

  }

  public void updateSetMultipleFields() {

    List<ODocument> result1 = database.command(new OCommandSQL("select salary from Account where salary is defined")).execute();
    Assert.assertFalse(result1.isEmpty());

    updatedRecords = database.command(
        new OCommandSQL("update Account set salary2 = salary, checkpoint = true where salary is defined")).execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result2 = database.command(new OCommandSQL("select from Account where salary is defined")).execute();
    Assert.assertFalse(result2.isEmpty());
    Assert.assertEquals(result2.size(), result1.size());

    for (int i = 0; i < result1.size(); ++i) {
      float salary1 = result1.get(i).field("salary");
      float salary2 = result2.get(i).field("salary2");
      Assert.assertEquals(salary2, salary1);
      Assert.assertEquals(result2.get(i).field("checkpoint"), true);
    }

  }

  public void updateAddMultipleFields() {

    updatedRecords = database.command(new OCommandSQL("update Account add myCollection = 1, myCollection = 2 limit 1")).execute();
    Assert.assertTrue(updatedRecords > 0);

    List<ODocument> result2 = database.command(new OCommandSQL("select from Account where myCollection is defined")).execute();
    Assert.assertEquals(result2.size(), 1);

    Collection<Object> myCollection = result2.iterator().next().field("myCollection");

    Assert.assertTrue(myCollection.containsAll(Arrays.asList(1, 2)));

  }

  public void testEscaping() {
    final OSchema schema = database.getMetadata().getSchema();
    schema.createClass("FormatEscapingTest");

    final ODocument document = new ODocument("FormatEscapingTest");
    document.save();

    database.command(
        new OCommandSQL("UPDATE FormatEscapingTest SET test = format('aaa \\' bbb') WHERE @rid = " + document.getIdentity()))
        .execute();

    document.reload();

    Assert.assertEquals(document.field("test"), "aaa ' bbb");

    database.command(
        new OCommandSQL("UPDATE FormatEscapingTest SET test = 'ccc \\' eee', test2 = format('aaa \\' bbb') WHERE @rid = "
            + document.getIdentity())).execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "ccc ' eee");
    Assert.assertEquals(document.field("test2"), "aaa ' bbb");

    database.command(new OCommandSQL("UPDATE FormatEscapingTest SET test = 'aaa \\n bbb' WHERE @rid = " + document.getIdentity()))
        .execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \n bbb");

    database.command(new OCommandSQL("UPDATE FormatEscapingTest SET test = 'aaa \\r bbb' WHERE @rid = " + document.getIdentity()))
        .execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \r bbb");

    database.command(new OCommandSQL("UPDATE FormatEscapingTest SET test = 'aaa \\b bbb' WHERE @rid = " + document.getIdentity()))
        .execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \b bbb");

    database.command(new OCommandSQL("UPDATE FormatEscapingTest SET test = 'aaa \\t bbb' WHERE @rid = " + document.getIdentity()))
        .execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \t bbb");

    database.command(new OCommandSQL("UPDATE FormatEscapingTest SET test = 'aaa \\f bbb' WHERE @rid = " + document.getIdentity()))
        .execute();

    document.reload();
    Assert.assertEquals(document.field("test"), "aaa \f bbb");
  }

  public void testUpdateVertexContent() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass vertex = schema.getClass("V");
    schema.createClass("UpdateVertexContent", vertex);

    final ORID vOneId = ((ODocument) database.command(new OCommandSQL("create vertex UpdateVertexContent")).execute())
        .getIdentity();
    final ORID vTwoId = ((ODocument) database.command(new OCommandSQL("create vertex UpdateVertexContent")).execute())
        .getIdentity();

    database.command(new OCommandSQL("create edge from " + vOneId + " to " + vTwoId)).execute();
    database.command(new OCommandSQL("create edge from " + vOneId + " to " + vTwoId)).execute();
    database.command(new OCommandSQL("create edge from " + vOneId + " to " + vTwoId)).execute();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select sum(outE().size(), inE().size()) from UpdateVertexContent"));

    Assert.assertEquals(result.size(), 2);

    for (ODocument doc : result) {
      Assert.assertEquals(doc.field("sum"), 3);
    }

    database.command(new OCommandSQL("update UpdateVertexContent content {value : 'val'} where @rid = " + vOneId)).execute();
    database.command(new OCommandSQL("update UpdateVertexContent content {value : 'val'} where @rid =  " + vTwoId)).execute();

    result = database.query(new OSQLSynchQuery<ODocument>("select sum(outE().size(), inE().size()) from UpdateVertexContent"));

    Assert.assertEquals(result.size(), 2);

    for (ODocument doc : result) {
      Assert.assertEquals(doc.field("sum"), 3);
    }

    result = database.query(new OSQLSynchQuery<ODocument>("select from UpdateVertexContent"));
    Assert.assertEquals(result.size(), 2);
    for (ODocument doc : result) {
      Assert.assertEquals(doc.field("value"), "val");
    }
  }

  public void testUpdateEdgeContent() {
    final OSchema schema = database.getMetadata().getSchema();
    OClass vertex = schema.getClass("V");
    OClass edge = schema.getClass("E");

    schema.createClass("UpdateEdgeContentV", vertex);
    schema.createClass("UpdateEdgeContentE", edge);

    final ORID vOneId = ((ODocument) database.command(new OCommandSQL("create vertex UpdateEdgeContentV")).execute()).getIdentity();
    final ORID vTwoId = ((ODocument) database.command(new OCommandSQL("create vertex UpdateEdgeContentV")).execute()).getIdentity();

    database.command(new OCommandSQL("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId)).execute();
    database.command(new OCommandSQL("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId)).execute();
    database.command(new OCommandSQL("create edge UpdateEdgeContentE from " + vOneId + " to " + vTwoId)).execute();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select outV(), inV() from UpdateEdgeContentE"));

    Assert.assertEquals(result.size(), 3);

    for (ODocument doc : result) {
      Assert.assertEquals(doc.field("outV"), vOneId);
      Assert.assertEquals(doc.field("inV"), vTwoId);
    }

    database.command(new OCommandSQL("update UpdateEdgeContentE content {value : 'val'}")).execute();

    result = database.query(new OSQLSynchQuery<ODocument>("select outV(), inV() from UpdateEdgeContentE"));

    Assert.assertEquals(result.size(), 3);

    for (ODocument doc : result) {
      Assert.assertEquals(doc.field("outV"), vOneId);
      Assert.assertEquals(doc.field("inV"), vTwoId);
    }

    result = database.query(new OSQLSynchQuery<ODocument>("select from UpdateEdgeContentE"));
    Assert.assertEquals(result.size(), 3);
    for (ODocument doc : result) {
      Assert.assertEquals(doc.field("value"), "val");
    }
  }

  private void checkUpdatedDoc(ODatabaseDocument database, String expectedName, String expectedCity, String expectedGender) {
    List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select * from person"));
    ODocument oDoc = result.get(0);
    Assert.assertEquals(expectedName, oDoc.field("name"));
    Assert.assertEquals(expectedCity, oDoc.field("city"));
    Assert.assertEquals(expectedGender, oDoc.field("gender"));
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final ORecordIteratorCluster<ODocument> iteratorCluster = database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 7; i++) {
      if (!iteratorCluster.hasNext())
        break;
      ODocument doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }

  public void testMultiplePut() {
    final ODocument v = database.newInstance("V").save();

    Integer records = database.command(
        new OCommandSQL("UPDATE " + v.getIdentity() + " PUT embmap = \"test\", \"Luca\" PUT embmap = \"test2\", \"Alex\""))
        .execute();

    Assert.assertEquals(records.intValue(), 1);

    v.reload();

    Assert.assertTrue(v.field("embmap") instanceof Map);
    Assert.assertEquals(((Map) v.field("embmap")).size(), 2);
  }

  public void testAutoConversionOfEmbeddededListWithLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    if (!c.existsProperty("embeddedListWithLinkedClass"))
      c.createProperty("embeddedListWithLinkedClass", OType.EMBEDDEDLIST,
          database.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    ODocument doc = database
        .command(
            new OCommandSQL(
                "INSERT INTO TestConvert SET name = 'embeddedListWithLinkedClass', embeddedListWithLinkedClass = [{'line1':'123 Fake Street'}]"))
        .execute();

    database.command(
        new OCommandSQL("UPDATE " + doc.getIdentity() + " ADD embeddedListWithLinkedClass = [{'line1':'123 Fake Street'}]"))
        .execute();

    doc.reload();

    Assert.assertTrue(doc.field("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.field("embeddedListWithLinkedClass")).size(), 2);

    database.command(
        new OCommandSQL("UPDATE " + doc.getIdentity() + " ADD embeddedListWithLinkedClass = {'line1':'123 Fake Street'}"))
        .execute();

    doc.reload();

    Assert.assertTrue(doc.field("embeddedListWithLinkedClass") instanceof List);
    Assert.assertEquals(((Collection) doc.field("embeddedListWithLinkedClass")).size(), 3);

    List addr = doc.field("embeddedListWithLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof ODocument);
      Assert.assertEquals(((ODocument) o).getClassName(), "TestConvertLinkedClass");
    }
  }
}
