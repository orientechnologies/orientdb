package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Ignore;
import org.junit.Test;

public class TestManyProperties {

  @Test
  @Ignore
  public void test() {
    try (OrientDB orientdb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientdb
          .execute("create database test memory users(admin identified by 'admin' role admin)")
          .close();
      try (ODatabaseSession session = orientdb.open("test", "admin", "admin")) {
        OClass clazz = session.createClass("test");
        clazz.createProperty("property1", OType.STRING);
        clazz.createProperty("property2", OType.STRING);
        clazz.createProperty("property3", OType.STRING);
        clazz.createProperty("property4", OType.STRING);
        clazz.createProperty("property5", OType.STRING);
        clazz.createProperty("property6", OType.STRING);
        clazz.createProperty("property7", OType.STRING);
        clazz.createProperty("property8", OType.STRING);
        clazz.createProperty("property9", OType.STRING);
        clazz.createProperty("property10", OType.STRING);
        clazz.createProperty("property11", OType.STRING);
        clazz.createProperty("property12", OType.STRING);
        clazz.createProperty("property13", OType.STRING);
        clazz.createProperty("property14", OType.STRING);
        clazz.createProperty("property15", OType.STRING);
        clazz.createProperty("property16", OType.STRING);
        clazz.createProperty("property17", OType.STRING);
        clazz.createProperty("property18", OType.STRING);
        clazz.createProperty("property19", OType.STRING);
        clazz.createProperty("property20", OType.STRING);
        clazz.createProperty("property21", OType.STRING);
        clazz.createProperty("property22", OType.STRING);
        clazz.createProperty("property23", OType.STRING);
        clazz.createProperty("property24", OType.STRING);

        try (OResultSet set =
            session.query(
                "SELECT FROM test WHERE (((property1 is null) or (property1 = #107:150)) and ((property2 is null) or (property2 = #107:150)) and ((property3 is null) or (property3 = #107:150)) and ((property4 is null) or (property4 = #107:150)) and ((property5 is null) or (property5 = #107:150)) and ((property6 is null) or (property6 = #107:150)) and ((property7 is null) or (property7 = #107:150)) and ((property8 is null) or (property8 = #107:150)) and ((property9 is null) or (property9 = #107:150)) and ((property10 is null) or (property10 = #107:150)) and ((property11 is null) or (property11 = #107:150)) and ((property12 is null) or (property12 = #107:150)) and ((property13 is null) or (property13 = #107:150)) and ((property14 is null) or (property14 = #107:150)) and ((property15 is null) or (property15 = #107:150)) and ((property16 is null) or (property16 = #107:150)) and ((property17 is null) or (property17 = #107:150)))")) {
          assertEquals(set.stream().count(), 0);
        }
      }
    }
  }
}
