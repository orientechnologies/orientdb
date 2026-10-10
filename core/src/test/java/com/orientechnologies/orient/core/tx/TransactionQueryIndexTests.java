package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 12/04/17. */
public class TransactionQueryIndexTests {

  private OrientDB orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() {
    orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    database = orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    res.close();
    res = database.query("select from Test where test='aaaaa' ");

    assertEquals(0L, res.stream().count());
    res.close();
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
    res.close();
    res = database.query("select from Test2 where foo='aaaaa' and bar = 'aaa'");

    assertEquals(0L, res.stream().count());
    res.close();
  }

  @Test
  public void testOneSidedRangeNotUnique() {
    testOneSidedRange("RangeNotUnique", OClass.INDEX_TYPE.NOTUNIQUE);
  }

  @Test
  public void testOneSidedRangeUnique() {
    testOneSidedRange("RangeUnique", OClass.INDEX_TYPE.UNIQUE);
  }

  private void testOneSidedRange(String className, OClass.INDEX_TYPE indexType) {
    OClass clazz = database.createClass(className);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("value", OType.LONG).createIndex(indexType);

    database.command("insert into " + className + " set name = 'a', value = -7").close();
    database.command("insert into " + className + " set name = 'b', value = 5").close();

    database.begin();
    database.command("update " + className + " set value = -100 where name = 'b'").close();
    assertEquals(2, countWhere(className, "value < 0"));
    assertEquals(2, countWhere(className, "value <= -7"));
    assertEquals(1, countWhere(className, "value < -7"));
    assertEquals(2, countWhere(className, "value > -200"));
    assertEquals(1, countWhere(className, "value >= -7"));
    assertEquals(0, countWhere(className, "value > 0"));
    database.rollback();

    database.begin();
    database.command("update " + className + " set value = 100 where name = 'a'").close();
    assertEquals(2, countWhere(className, "value > 0"));
    assertEquals(2, countWhere(className, "value >= 5"));
    assertEquals(1, countWhere(className, "value > 5"));
    assertEquals(2, countWhere(className, "value < 200"));
    assertEquals(1, countWhere(className, "value <= 5"));
    assertEquals(0, countWhere(className, "value < 0"));
    database.rollback();
  }

  private long countWhere(String className, String condition) {
    try (OResultSet res = database.query("select from " + className + " where " + condition)) {
      return res.stream().count();
    }
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
  }
}
