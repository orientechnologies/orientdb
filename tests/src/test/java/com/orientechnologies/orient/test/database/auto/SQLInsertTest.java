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

import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.*;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be because the order of clusters could
 * be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-insert")
public class SQLInsertTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLInsertTest(@Optional String url) {
    super(url);
  }

  @Test
  public void insertOperator() {
    final int clId = database.addCluster("anotherdefault");
    final OClass profileClass = database.getMetadata().getSchema().getClass("Account");
    profileClass.addClusterId(clId);

    int addressId = database.getMetadata().getSchema().getClass("Address").getDefaultClusterId();

    List<Long> positions = getValidPositions(addressId);

    ODocument doc = (ODocument) database.command(
        new OCommandSQL("insert into Profile (name, surname, salary, location, dummy) values ('Luca','Smith', 109.9, #" + addressId
            + ":" + positions.get(3) + ", 'hooray')")).execute();

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("name"), "Luca");
    Assert.assertEquals(doc.field("surname"), "Smith");
    Assert.assertEquals(((Number) doc.field("salary")).floatValue(), 109.9f);
    Assert.assertEquals(doc.field("location"), new ORecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.field("dummy"), "hooray");

    doc = (ODocument) database.command(
        new OCommandSQL("insert into Profile SET name = 'Luca', surname = 'Smith', salary = 109.9, location = #" + addressId + ":"
            + positions.get(3) + ", dummy =  'hooray'")).execute();

    database.delete(doc);

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("name"), "Luca");
    Assert.assertEquals(doc.field("surname"), "Smith");
    Assert.assertEquals(((Number) doc.field("salary")).floatValue(), 109.9f);
    Assert.assertEquals(doc.field("location", OType.LINK), new ORecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.field("dummy"), "hooray");
  }

  @Test
  public void insertWithWildcards() {
    int addressId = database.getMetadata().getSchema().getClass("Address").getDefaultClusterId();

    List<Long> positions = getValidPositions(addressId);

    ODocument doc = (ODocument) database.command(
        new OCommandSQL("insert into Profile (name, surname, salary, location, dummy) values (?,?,?,?,?)")).execute("Marc",
        "Smith", 120.0, new ORecordId(addressId, positions.get(3)), "hooray");

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("name"), "Marc");
    Assert.assertEquals(doc.field("surname"), "Smith");
    Assert.assertEquals(((Number) doc.field("salary")).floatValue(), 120.0f);
    Assert.assertEquals(doc.field("location"), new ORecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.field("dummy"), "hooray");

    database.delete(doc);

    doc = (ODocument) database.command(
        new OCommandSQL("insert into Profile SET name = ?, surname = ?, salary = ?, location = ?, dummy = ?")).execute("Marc",
        "Smith", 120.0, new ORecordId(addressId, positions.get(3)), "hooray");

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("name"), "Marc");
    Assert.assertEquals(doc.field("surname"), "Smith");
    Assert.assertEquals(((Number) doc.field("salary")).floatValue(), 120.0f);
    Assert.assertEquals(doc.field("location", OType.LINK), new ORecordId(addressId, positions.get(3)));
    Assert.assertEquals(doc.field("dummy"), "hooray");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void insertMap() {
    ODocument doc = (ODocument) database
        .command(
            new OCommandSQL(
                "insert into cluster:default (equaledges, name, properties) values ('no', 'circle', {'round':'eeee', 'blaaa':'zigzag'} )"))
        .execute();

    Assert.assertTrue(doc != null);

    doc = (ODocument) new ODocument(doc.getIdentity()).load();

    Assert.assertEquals(doc.field("equaledges"), "no");
    Assert.assertEquals(doc.field("name"), "circle");
    Assert.assertTrue(doc.field("properties") instanceof Map);

    Map<Object, Object> entries = ((Map<Object, Object>) doc.field("properties"));
    Assert.assertEquals(entries.size(), 2);

    Assert.assertEquals(entries.get("round"), "eeee");
    Assert.assertEquals(entries.get("blaaa"), "zigzag");

    database.delete(doc);

    doc = (ODocument) database
        .command(
            new OCommandSQL(
                "insert into cluster:default SET equaledges = 'no', name = 'circle', properties = {'round':'eeee', 'blaaa':'zigzag'} "))
        .execute();

    Assert.assertTrue(doc != null);

    doc = (ODocument) new ODocument(doc.getIdentity()).load();

    Assert.assertEquals(doc.field("equaledges"), "no");
    Assert.assertEquals(doc.field("name"), "circle");
    Assert.assertTrue(doc.field("properties") instanceof Map);

    entries = ((Map<Object, Object>) doc.field("properties"));
    Assert.assertEquals(entries.size(), 2);

    Assert.assertEquals(entries.get("round"), "eeee");
    Assert.assertEquals(entries.get("blaaa"), "zigzag");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void insertList() {
    ODocument doc = (ODocument) database.command(
        new OCommandSQL(
            "insert into cluster:default (equaledges, name, list) values ('yes', 'square', ['bottom', 'top','left','right'] )"))
        .execute();

    Assert.assertTrue(doc != null);

    doc = (ODocument) new ODocument(doc.getIdentity()).load();

    Assert.assertEquals(doc.field("equaledges"), "yes");
    Assert.assertEquals(doc.field("name"), "square");
    Assert.assertTrue(doc.field("list") instanceof List);

    List<Object> entries = ((List<Object>) doc.field("list"));
    Assert.assertEquals(entries.size(), 4);

    Assert.assertEquals(entries.get(0), "bottom");
    Assert.assertEquals(entries.get(1), "top");
    Assert.assertEquals(entries.get(2), "left");
    Assert.assertEquals(entries.get(3), "right");

    database.delete(doc);

    doc = (ODocument) database.command(
        new OCommandSQL(
            "insert into cluster:default SET equaledges = 'yes', name = 'square', list = ['bottom', 'top','left','right'] "))
        .execute();

    Assert.assertTrue(doc != null);

    doc = (ODocument) new ODocument(doc.getIdentity()).load();

    Assert.assertEquals(doc.field("equaledges"), "yes");
    Assert.assertEquals(doc.field("name"), "square");
    Assert.assertTrue(doc.field("list") instanceof List);

    entries = ((List<Object>) doc.field("list"));
    Assert.assertEquals(entries.size(), 4);

    Assert.assertEquals(entries.get(0), "bottom");
    Assert.assertEquals(entries.get(1), "top");
    Assert.assertEquals(entries.get(2), "left");
    Assert.assertEquals(entries.get(3), "right");
  }

  @Test
  public void insertWithNoSpaces() {
    ODocument doc = (ODocument) database.command(
        new OCommandSQL("insert into cluster:default(id, title)values(10, 'NoSQL movement')")).execute();

    Assert.assertTrue(doc != null);
  }

  @Test
  public void insertAvoidingSubQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    if (schema.getClass("test") == null)
      schema.createClass("test");

    ODocument doc = (ODocument) database.command(new OCommandSQL("INSERT INTO test(text) VALUES ('(Hello World)')")).execute();

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("text"), "(Hello World)");
  }

  @Test
  public void insertSubQuery() {
    final OSchema schema = database.getMetadata().getSchema();
    if (schema.getClass("test") == null)
      schema.createClass("test");

    final List<ODocument> usersCount = database.query(new OSQLSynchQuery<ODocument>("select count(*) from OUser"));
    final long uCount = usersCount.get(0).field("count");

    ODocument doc = (ODocument) database.command(new OCommandSQL("INSERT INTO test SET names = (select name from OUser)"))
        .execute();

    Assert.assertTrue(doc != null);
    Assert.assertNotNull(doc.field("names"));
    Assert.assertTrue(doc.field("names") instanceof Collection);
    Assert.assertEquals(((Collection<?>) doc.field("names")).size(), uCount);
  }

  @Test(dependsOnMethods = "insertOperator")
  public void insertCluster() {
    ODocument doc = database.command(
        new OCommandSQL("insert into Account cluster anotherdefault (id, title) values (10, 'NoSQL movement')")).execute();

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.getIdentity().getClusterId(), database.getClusterIdByName("anotherdefault"));
    Assert.assertEquals(doc.getClassName(), "Account");
  }

  public void updateMultipleFields() {
    List<Long> positions = getValidPositions(3);

    OIdentifiable result = database.command(
        new OCommandSQL("  INSERT INTO Account SET id= 3232,name= 'my name',map= {\"key\":\"value\"},dir= '',user= #3:"
            + positions.get(0))).execute();
    Assert.assertNotNull(result);

    ODocument record = result.getRecord();

    Assert.assertEquals(record.field("id"), 3232);
    Assert.assertEquals(record.field("name"), "my name");
    Map<String, String> map = record.field("map");
    Assert.assertTrue(map.get("key").equals("value"));
    Assert.assertEquals(record.field("dir"), "");
    Assert.assertEquals(record.field("user"), new ORecordId(3, positions.get(0)));
  }

  public void insertSelect() {
    database.command(new OCommandSQL("CREATE CLASS UserCopy")).execute();
    database.getMetadata().getSchema().reload();

    long inserted = database.command(new OCommandSQL("INSERT INTO UserCopy FROM select from ouser where name <> 'admin' limit 2"))
        .execute();
    Assert.assertEquals(inserted, 2);

    List<OIdentifiable> result = database.query(new OSQLSynchQuery<OIdentifiable>("select from UserCopy"));
    Assert.assertEquals(result.size(), 2);
    for (OIdentifiable r : result) {
      Assert.assertEquals(((ODocument) r.getRecord()).getClassName(), "UserCopy");
      Assert.assertNotSame(((ODocument) r.getRecord()).field("name"), "admin");
    }
  }

  public void insertWithReturn() {

    if (!database.getMetadata().getSchema().existsClass("actor2")) {
      database.command(new OCommandSQL("CREATE CLASS Actor2")).execute();
      database.getMetadata().getSchema().reload();
    }

    // RETURN with $current.
    ODocument doc = database.command(new OCommandSQL("INSERT INTO Actor2 SET FirstName=\"FFFF\" RETURN $current")).execute();
    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.getClassName(), "Actor2");

    // RETURN with @rid
    Object res1 = database.command(new OCommandSQL("INSERT INTO Actor2 SET FirstName=\"Butch 1\" RETURN @rid")).execute();
    Assert.assertTrue(res1 instanceof ORecordId);
    Assert.assertTrue(((OIdentifiable) res1).getIdentity().isValid());

    // Create many records and return @rid
    Object res2 = database.command(
        new OCommandSQL(
            "INSERT INTO Actor2(FirstName,LastName) VALUES ('Jay','Miner'),('Frank','Hermier'),('Emily','Saut')  RETURN @rid"))
        .execute();
    Assert.assertTrue(res2 instanceof List<?>);
    Assert.assertTrue(((List) res2).get(0) instanceof ORecordId);

    // Create many records by INSERT INTO ...FROM and return wrapped field
    ORID another = ((OIdentifiable) res1).getIdentity();
    final String sql = "INSERT INTO Actor2 RETURN $current.FirstName  FROM SELECT * FROM [" + doc.getIdentity().toString() + ","
        + another.toString() + "]";
    ArrayList res3 = database.command(new OCommandSQL(sql)).execute();
    Assert.assertEquals(res3.size(), 2);
    Assert.assertTrue(((List) res3).get(0) instanceof ODocument);
    final ODocument res3doc = (ODocument) res3.get(0);
    Assert.assertTrue(res3doc.containsField("result"));
    Assert.assertTrue("FFFF".equalsIgnoreCase((String) res3doc.field("result"))
        || "Butch 1".equalsIgnoreCase((String) res3doc.field("result")));
    Assert.assertTrue(res3doc.containsField("rid"));
    Assert.assertTrue(res3doc.containsField("version"));

    // create record using content keyword and update it in sql batch passing recordID between commands
    final String sql2 = "let var1=INSERT INTO Actor2 CONTENT {Name:\"content\"} RETURN $current.@rid\n"
        + "let var2=UPDATE $var1 SET Bingo=1 RETURN AFTER @rid\n" + "return $var2";
    List<?> res_sql2 = database.command(new OCommandScript("sql", sql2)).execute();
    Assert.assertEquals(res_sql2.size(), 1);
    Assert.assertTrue(((List) res_sql2).get(0) instanceof ORecordId);

    // create record using content keyword and update it in sql batch passing recordID between commands
    final String sql3 = "let var1=INSERT INTO Actor2 CONTENT {Name:\"Bingo owner\"} RETURN @this\n"
        + "let var2=UPDATE $var1 SET Bingo=1 RETURN AFTER\n" + "return $var2";
    List<?> res_sql3 = database.command(new OCommandScript("sql", sql3)).execute();
    Assert.assertEquals(res_sql3.size(), 1);
    Assert.assertTrue(((List) res_sql3).get(0) instanceof ODocument);
    final ODocument sql3doc = (ODocument) (((List) res_sql3).get(0));
    Assert.assertEquals(sql3doc.field("Bingo"), 1);
    Assert.assertEquals(sql3doc.field("Name"), "Bingo owner");
  }

  public void testAutoConversionOfEmbeddededSetNoLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty("embeddedSetNoLinkedClass", OType.EMBEDDEDSET);

    ODocument doc = database
        .command(
            new OCommandSQL(
                "INSERT INTO TestConvert SET name = 'embeddedSetNoLinkedClass', embeddedSetNoLinkedClass = [{'line1':'123 Fake Street'}]"))
        .execute();

    Assert.assertTrue(doc.field("embeddedSetNoLinkedClass") instanceof Set);

    Set addr = doc.field("embeddedSetNoLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof ODocument);
    }
  }

  public void testAutoConversionOfEmbeddededSetWithLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty("embeddedSetWithLinkedClass", OType.EMBEDDEDSET,
        database.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    ODocument doc = database
        .command(
            new OCommandSQL(
                "INSERT INTO TestConvert SET name = 'embeddedSetWithLinkedClass', embeddedSetWithLinkedClass = [{'line1':'123 Fake Street'}]"))
        .execute();

    Assert.assertTrue(doc.field("embeddedSetWithLinkedClass") instanceof Set);

    Set addr = doc.field("embeddedSetWithLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof ODocument);
      Assert.assertEquals(((ODocument) o).getClassName(), "TestConvertLinkedClass");
    }
  }

  public void testAutoConversionOfEmbeddededListNoLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty("embeddedListNoLinkedClass", OType.EMBEDDEDLIST);

    ODocument doc = database
        .command(
            new OCommandSQL(
                "INSERT INTO TestConvert SET name = 'embeddedListNoLinkedClass', embeddedListNoLinkedClass = [{'line1':'123 Fake Street'}]"))
        .execute();

    Assert.assertTrue(doc.field("embeddedListNoLinkedClass") instanceof List);

    List addr = doc.field("embeddedListNoLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof ODocument);
    }
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

    Assert.assertTrue(doc.field("embeddedListWithLinkedClass") instanceof List);

    List addr = doc.field("embeddedListWithLinkedClass");
    for (Object o : addr) {
      Assert.assertTrue(o instanceof ODocument);
      Assert.assertEquals(((ODocument) o).getClassName(), "TestConvertLinkedClass");
    }
  }

  public void testAutoConversionOfEmbeddededMapNoLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty("embeddedMapNoLinkedClass", OType.EMBEDDEDMAP);

    ODocument doc = database
        .command(
            new OCommandSQL(
                "INSERT INTO TestConvert SET name = 'embeddedMapNoLinkedClass', embeddedMapNoLinkedClass = {test:{'line1':'123 Fake Street'}}"))
        .execute();

    Assert.assertTrue(doc.field("embeddedMapNoLinkedClass") instanceof Map);

    Map addr = doc.field("embeddedMapNoLinkedClass");
    for (Object o : addr.values()) {
      Assert.assertTrue(o instanceof ODocument);
    }
  }

  public void testAutoConversionOfEmbeddededMapWithLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty("embeddedMapWithLinkedClass", OType.EMBEDDEDMAP,
        database.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    ODocument doc = database
        .command(
            new OCommandSQL(
                "INSERT INTO TestConvert SET name = 'embeddedMapWithLinkedClass', embeddedMapWithLinkedClass = {test:{'line1':'123 Fake Street'}}"))
        .execute();

    Assert.assertTrue(doc.field("embeddedMapWithLinkedClass") instanceof Map);

    Map addr = doc.field("embeddedMapWithLinkedClass");
    for (Object o : addr.values()) {
      Assert.assertTrue(o instanceof ODocument);
      Assert.assertEquals(((ODocument) o).getClassName(), "TestConvertLinkedClass");
    }
  }

  public void testAutoConversionOfEmbeddededNoLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty("embeddedNoLinkedClass", OType.EMBEDDED);

    ODocument doc = database.command(
        new OCommandSQL(
            "INSERT INTO TestConvert SET name = 'embeddedNoLinkedClass', embeddedNoLinkedClass = {'line1':'123 Fake Street'}"))
        .execute();

    Assert.assertTrue(doc.field("embeddedNoLinkedClass") instanceof ODocument);
  }

  public void testEmbeddedDates() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestEmbeddedDates");

    database
        .command(
            new OCommandSQL(
                "insert into TestEmbeddedDates set events = [{\"on\": date(\"2005-09-08 04:00:00\", \"yyyy-MM-dd HH:mm:ss\", \"UTC\")}]\n"))
        .execute();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from TestEmbeddedDates"));

    Assert.assertEquals(result.size(), 1);
    boolean found = false;
    ODocument doc = result.get(0);
    Collection events = doc.field("events");
    for (Object event : events) {
      Assert.assertTrue(event instanceof Map);
      Object dateObj = ((Map) event).get("on");
      Assert.assertTrue(dateObj instanceof Date);
      Calendar cal = new GregorianCalendar();
      cal.setTime((Date) dateObj);
      Assert.assertEquals(cal.get(Calendar.YEAR), 2005);
      found = true;
    }

    doc.delete();
    Assert.assertEquals(found, true);

  }

  public void testAutoConversionOfEmbeddededWithLinkedClass() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("TestConvert");
    c.createProperty("embeddedWithLinkedClass", OType.EMBEDDED,
        database.getMetadata().getSchema().getOrCreateClass("TestConvertLinkedClass"));

    ODocument doc = database.command(
        new OCommandSQL(
            "INSERT INTO TestConvert SET name = 'embeddedWithLinkedClass', embeddedWithLinkedClass = {'line1':'123 Fake Street'}"))
        .execute();

    Assert.assertTrue(doc.field("embeddedWithLinkedClass") instanceof ODocument);
    Assert.assertEquals(((ODocument) doc.field("embeddedWithLinkedClass")).getClassName(), "TestConvertLinkedClass");
  }

  public void testInsertEmbeddedWithRecordAttributes() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("EmbeddedWithRecordAttributes");
    c.createProperty("like", OType.EMBEDDED,
        database.getMetadata().getSchema().getOrCreateClass("EmbeddedWithRecordAttributes_Like"));

    ODocument doc = database.command(
        new OCommandSQL("INSERT INTO EmbeddedWithRecordAttributes SET `like` = { \n" + "      count: 0, \n"
            + "      latest: [], \n" + "      '@type': 'document', \n" + "      '@class': 'EmbeddedWithRecordAttributes_Like'\n"
            + "    } ")).execute();

    Assert.assertTrue(doc.field("like") instanceof OIdentifiable);
    Assert.assertEquals(((ODocument) doc.field("like")).getClassName(), "EmbeddedWithRecordAttributes_Like");
    Assert.assertEquals(((ODocument) doc.field("like")).field("count"), 0);
  }

  public void testInsertEmbeddedWithRecordAttributes2() {
    OClass c = database.getMetadata().getSchema().getOrCreateClass("EmbeddedWithRecordAttributes2");
    c.createProperty("like", OType.EMBEDDED,
        database.getMetadata().getSchema().getOrCreateClass("EmbeddedWithRecordAttributes2_Like"));

    ODocument doc = database.command(
        new OCommandSQL("INSERT INTO EmbeddedWithRecordAttributes2 SET `like` = { \n" + "      count: 0, \n"
            + "      latest: [], \n" + "      @type: 'document', \n" + "      @class: 'EmbeddedWithRecordAttributes2_Like'\n"
            + "    } ")).execute();

    Assert.assertTrue(doc.field("like") instanceof OIdentifiable);
    Assert.assertEquals(((ODocument) doc.field("like")).getClassName(), "EmbeddedWithRecordAttributes2_Like");
    Assert.assertEquals(((ODocument) doc.field("like")).field("count"), 0);
  }

  private List<Long> getValidPositions(int clusterId) {
    final List<Long> positions = new ArrayList<Long>();

    final ORecordIteratorCluster<?> iteratorCluster = database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 100; i++) {
      if (!iteratorCluster.hasNext())
        break;
      ORecord doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }
}
