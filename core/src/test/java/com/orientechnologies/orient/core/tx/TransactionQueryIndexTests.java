package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 12/04/17.
 */
public class TransactionQueryIndexTests {

  private OrientDB          orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    database = orientDB.open("test", "admin", "admin");
  }

  @Test
  public void test() {
    OClass clazz = database.createClass("test");
    OProperty prop = clazz.createProperty("test", OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    database.begin();
    ODocument doc = database.newInstance("test");
    doc.setProperty("test", "abcdefg");
    database.save(doc);
    OResultSet res = database.query("select from Test where test='abcdefg' ");

    assertEquals(1L, res.stream().count());

    res = database.query("select from Test where test='aaaaa' ");

    System.out.println(res.getExecutionPlan().get().prettyPrint(0, 0));
    assertEquals(0L, res.stream().count());
  }

  @Test
  public void test2() {
    OClass clazz = database.createClass("Test2");
    clazz.createProperty("foo", OType.STRING);
    clazz.createProperty("bar", OType.STRING);
    clazz.createIndex("Test2.foo_bar", OClass.INDEX_TYPE.NOTUNIQUE, "foo", "bar");

    database.begin();
    ODocument doc = database.newInstance("Test2");
    doc.setProperty("foo", "abcdefg");
    doc.setProperty("bar", "abcdefg");
    database.save(doc);
    OResultSet res = database.query("select from Test2 where foo='abcdefg' and bar = 'abcdefg' ");

    assertEquals(1L, res.stream().count());

    res = database.query("select from Test2 where foo='aaaaa' and bar = 'aaa'");

    System.out.println(res.getExecutionPlan().get().prettyPrint(0, 0));
    assertEquals(0L, res.stream().count());
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
  }

}
