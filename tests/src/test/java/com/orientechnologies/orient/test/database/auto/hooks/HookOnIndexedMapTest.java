package com.orientechnologies.orient.test.database.auto.hooks;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class HookOnIndexedMapTest {

  @Test
  public void test() {
    OrientDB orient = new OrientDB("plocal:.", "root", "root", OrientDBConfig.defaultConfig());

    orient.execute(
        "create database " + "test" + " memory users ( admin identified by 'admin' role admin)");
    ODatabaseSession db = orient.open("test", "admin", "admin");
    db.registerHook(new BrokenMapHook());

    db.command("CREATE CLASS AbsVertex IF NOT EXISTS EXTENDS V ABSTRACT;");
    db.command("CREATE PROPERTY AbsVertex.uId IF NOT EXISTS string;");
    db.command("CREATE PROPERTY AbsVertex.myMap IF NOT EXISTS EMBEDDEDMAP;");

    db.command("CREATE CLASS MyClass IF NOT EXISTS EXTENDS AbsVertex;");
    db.command("CREATE INDEX MyClass.uId IF NOT EXISTS ON MyClass(uId) UNIQUE;");
    db.command("CREATE INDEX MyClass.myMap IF NOT EXISTS ON MyClass(myMap by key) NOTUNIQUE;");

    db.command("INSERT INTO MyClass SET uId = \"test1\", myMap={\"F1\": \"V1\"}");

    try (OResultSet rs = db.command("SELECT * FROM INDEX:MyClass.myMap ORDER BY rid")) {
      //      System.out.println("----------");
      //      System.out.println("SELECT * FROM INDEX:MyClass.myMap ORDER BY rid");
      //      rs.forEachRemaining(x-> System.out.println(x));
    }

    try (OResultSet rs = db.command("SELECT FROM V")) {
      //      System.out.println("----------");
      //      System.out.println("SELECT FROM V");
      //      rs.forEachRemaining(x-> System.out.println(x));
    }

    db.command("UPDATE MyClass SET myMap = {\"F11\": \"V11\"} WHERE uId = \"test1\"");

    try (OResultSet rs = db.command("SELECT FROM V")) {
      //      System.out.println("----------");
      //      System.out.println("SELECT FROM V");
      //      rs.forEachRemaining(x-> System.out.println(x));
    }

    try (OResultSet rs = db.command("SELECT * FROM INDEX:MyClass.myMap ORDER BY rid")) {
      //      System.out.println("----------");
      //      System.out.println("SELECT * FROM INDEX:MyClass.myMap ORDER BY rid");
      //      rs.forEachRemaining(x-> System.out.println(x));
    }

    try (OResultSet rs = db.command("SELECT COUNT(*) FROM MyClass WHERE myMap.F1 IS NOT NULL")) {
      //      System.out.println("----------");
      //      System.out.println("SELECT COUNT(*) FROM MyClass WHERE myMap.F1 IS NOT NULL");
      //      rs.forEachRemaining(x-> System.out.println(x));
    }

    try (OResultSet rs = db.query("SELECT COUNT(*) FROM MyClass WHERE myMap CONTAINSKEY 'F1'")) {
      //      System.out.println("----------");
      //      System.out.println("SELECT COUNT(*) FROM MyClass WHERE myMap CONTAINSKEY 'F1'");
      //      rs.forEachRemaining(x-> System.out.println(x));
    }

    db.command("DELETE VERTEX FROM V");

    try (OResultSet rs = db.command("SELECT * FROM INDEX:MyClass.myMap ORDER BY rid")) {
      //      System.out.println("----------");
      //      System.out.println("SELECT * FROM INDEX:MyClass.myMap ORDER BY rid");
      if (rs.hasNext()) {
        //        System.out.println(rs.next());
        Assert.fail();
      }
    }
  }
}
