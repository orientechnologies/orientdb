package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BigKeyIndexTest {

  private OrientDB orientDB;
  private ODatabaseDocument db;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute(
        " create database ? memory users (admin identified by 'adminpwd' role admin) ",
        BigKeyIndexTest.class.getSimpleName());
    db = orientDB.open(BigKeyIndexTest.class.getSimpleName(), "admin", "adminpwd");
  }

  @After
  public void after() {
    db.close();
    orientDB.drop(BigKeyIndexTest.class.getSimpleName());
    orientDB.close();
  }

  @Test
  public void testBigKey() {
    OClass cl = db.createClass("One");
    OProperty prop = cl.createProperty("two", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 100; i++) {
      ODocument doc = db.newInstance("One");
      String bigValue = i % 1000 + "one10000";
      for (int z = 0; z < 1000; z++) {
        bigValue += "one" + z;
      }
      System.out.println("" + bigValue.length());
      doc.setProperty("two", bigValue);
      db.save(doc);
    }
  }

  @Test(expected = OTooBigIndexKeyException.class)
  public void testTooBigKey() {
    OClass cl = db.createClass("One");
    OProperty prop = cl.createProperty("two", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    ODocument doc = db.newInstance("One");
    String bigValue = "";
    for (int z = 0; z < 3000; z++) {
      bigValue += "one" + z;
    }
    System.out.println("" + bigValue.length());
    doc.setProperty("two", bigValue);
    db.save(doc);
  }
}
