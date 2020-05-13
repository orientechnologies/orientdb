package com.orientechnologies;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

public class Foo {

  @Test
  public void test() {
    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists("TestGraphOperations", ODatabaseType.MEMORY);
    ODatabaseSession db = orientDB.open("TestGraphOperations", "admin", "admin");
    db.createVertexClass("Foo");

    int size = 500_000;

    for (int i = 0; i < size; i++) {
      OVertex v = db.newVertex("Foo");
      for (int j = 0; j < 30; j++) {
        v.setProperty("foo" + j, "foo " + j);
      }
      v.save();
    }

    System.out.println("write done");
    long prev = System.currentTimeMillis();

    long count = 0;

    boolean finished = false;
    while (!finished) {
      try (OResultSet rs = db.query("select from Foo")) {
        while (rs.hasNext()) {
          OResult item = rs.next();
          if (item.getProperty("foo0").equals("bar")) {
            System.out.println("wat...");
            finished = true;
          }
          count++;

          if (count % size == 0) {
            System.out.println("" + size + " in " + (System.currentTimeMillis() - prev) + " millis");
            prev = System.currentTimeMillis();
          }
        }
      }

    }
    db.close();
    orientDB.close();
  }
}
