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
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be because the order of clusters could
 * be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-insert")
public class SQLInsertTest {
  private ODatabaseDocument database;

  @Parameters(value = "url")
  public SQLInsertTest(String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @Test
  public void insertOperator() {
    database.open("admin", "admin");

    int addressId = database.getMetadata().getSchema().getClass("Address").getDefaultClusterId();

    List<OClusterPosition> positions = getValidPositions(addressId);

    ODocument doc = (ODocument) database.command(
        new OCommandSQL("insert into Profile (name, surname, salary, location, dummy) values ('Luca','Smith', 109.9, #" + addressId
            + ":" + positions.get(3) + ", 'hooray')")).execute();

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("name"), "Luca");
    Assert.assertEquals(doc.field("surname"), "Smith");
    Assert.assertEquals(((Number) doc.field("salary")).floatValue(), 109.9f);
    Assert.assertEquals(doc.field("location", OType.LINK), new ORecordId(addressId, positions.get(3)));
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

    database.close();
  }

  @Test
  public void insertWithWildcards() {
    database.open("admin", "admin");

    int addressId = database.getMetadata().getSchema().getClass("Address").getDefaultClusterId();

    List<OClusterPosition> positions = getValidPositions(addressId);

    ODocument doc = (ODocument) database.command(
        new OCommandSQL("insert into Profile (name, surname, salary, location, dummy) values (?,?,?,?,?)")).execute("Marc",
        "Smith", 120.0, new ORecordId(addressId, positions.get(3)), "hooray");

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("name"), "Marc");
    Assert.assertEquals(doc.field("surname"), "Smith");
    Assert.assertEquals(((Number) doc.field("salary")).floatValue(), 120.0f);
    Assert.assertEquals(doc.field("location", OType.LINK), new ORecordId(addressId, positions.get(3)));
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

    database.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void insertMap() {
    database.open("admin", "admin");

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
    database.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void insertList() {
    database.open("admin", "admin");

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

    database.close();
  }

  @Test
  public void insertWithNoSpaces() {
    database.open("admin", "admin");

    ODocument doc = (ODocument) database.command(
        new OCommandSQL("insert into cluster:default(id, title)values(10, 'NoSQL movement')")).execute();

    Assert.assertTrue(doc != null);

    database.close();
  }

  @Test
  public void insertAvoidingSubQuery() {
    database.open("admin", "admin");

    final OSchema schema = database.getMetadata().getSchema();
    if (schema.getClass("test") == null)
      schema.createClass("test");

    ODocument doc = (ODocument) database.command(new OCommandSQL("INSERT INTO test(text) VALUES ('(Hello World)')")).execute();

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.field("text"), "(Hello World)");

    database.close();
  }

  @Test
  public void insertSubQuery() {
    database.open("admin", "admin");

    final OSchema schema = database.getMetadata().getSchema();
    if (schema.getClass("test") == null)
      schema.createClass("test");

    ODocument doc = (ODocument) database.command(new OCommandSQL("INSERT INTO test SET names = (select name from OUser)"))
        .execute();

    Assert.assertTrue(doc != null);
    Assert.assertNotNull(doc.field("names"));
    Assert.assertTrue(doc.field("names") instanceof Collection);
    Assert.assertEquals(((Collection<?>) doc.field("names")).size(), 3);

    database.close();
  }

  @Test
  public void insertCluster() {
    database.open("admin", "admin");

    ODocument doc = (ODocument) database.command(
        new OCommandSQL("insert into Account cluster default (id, title) values (10, 'NoSQL movement')")).execute();

    Assert.assertTrue(doc != null);
    Assert.assertEquals(doc.getIdentity().getClusterId(), database.getDefaultClusterId());
    Assert.assertEquals(doc.getClassName(), "Account");

    database.close();
  }

  public void updateMultipleFields() {
    database.open("admin", "admin");

    List<OClusterPosition> positions = getValidPositions(3);

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
    Assert.assertEquals(record.field("user", OType.LINK), new ORecordId(3, positions.get(0)));

    database.close();
  }

  private List<OClusterPosition> getValidPositions(int clusterId) {
    final List<OClusterPosition> positions = new ArrayList<OClusterPosition>();

    final ORecordIteratorCluster<?> iteratorCluster = database.browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 100; i++) {
      if (!iteratorCluster.hasNext())
        break;
      ORecord<?> doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }
}
