package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OViewRefreshIT {
  private OrientDB orientDB;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB
        .execute(
            "create database ? memory users (admin identified by 'admpwd' role admin)",
            this.getClass().getSimpleName())
        .close();
  }

  public void simpleViewRefresh() {}

  @Test
  public void testLiveUpdateInsert() throws InterruptedException {
    try (ODatabaseSession db = orientDB.open(this.getClass().getSimpleName(), "admin", "admpwd")) {
      String className = "testLiveUpdateInsertClass";
      String viewName = "testLiveUpdateInsert";
      db.createClass(className);

      for (int i = 0; i < 10; i++) {
        OElement elem = db.newElement(className);
        elem.setProperty("name", "name" + i);
        elem.setProperty("surname", "surname" + i);
        elem.save();
      }

      String statement =
          "CREATE VIEW "
              + viewName
              + " FROM (SELECT FROM "
              + className
              + ") METADATA {\"updateStrategy\":\"live\"}";

      db.command(statement);

      Thread.sleep(2000);

      OResultSet result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(10, result.stream().count());
      result.close();

      db.command("insert into " + className + " set name = 'name10', surname = 'surname10'");
      Thread.sleep(1000);
      result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(11, result.stream().count());
      result.close();
    }
  }

  @Test
  public void testMultipleInsert() throws InterruptedException {
    try (ODatabaseSession db = orientDB.open(this.getClass().getSimpleName(), "admin", "admpwd")) {
      String className = "testUpdateInsertClass";
      String viewName = "testUpdateInsert";
      db.createClass(className);

      for (int i = 0; i < 10; i++) {
        OElement elem = db.newElement(className);
        elem.setProperty("name", "name" + i);
        elem.setProperty("surname", "surname" + i);
        elem.save();
      }

      String statement =
          "CREATE VIEW "
              + viewName
              + " FROM (SELECT FROM "
              + className
              + ") metadata {\"updateIntervalSeconds\":1} ";

      db.command(statement);

      Thread.sleep(2000);

      OResultSet result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(10, result.stream().count());
      result.close();

      for (int i = 10; i < 20; i++) {
        OElement elem = db.newElement(className);
        elem.setProperty("name", "name" + i);
        elem.setProperty("surname", "surname" + i);
        elem.save();
      }

      Thread.sleep(3000);
      result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(20, result.stream().count());
      result.close();
    }
  }

  @Test
  public void testUdpateDeleted() throws InterruptedException {
    try (ODatabaseSession db = orientDB.open(this.getClass().getSimpleName(), "admin", "admpwd")) {
      String className = "testUpdateDeleteClass";
      String viewName = "testUpdateDelete";
      db.createClass(className);

      for (int i = 0; i < 10; i++) {
        OElement elem = db.newElement(className);
        elem.setProperty("name", "name" + i);
        elem.setProperty("surname", "surname" + i);
        elem.save();
      }

      String statement =
          "CREATE VIEW "
              + viewName
              + " FROM (SELECT FROM "
              + className
              + ") metadata {\"updateIntervalSeconds\":1} ";

      db.command(statement);

      Thread.sleep(2000);

      OResultSet result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(10, result.stream().count());
      result.close();

      db.command("delete FROM " + className + " where name in [\"name2\",\"name6\"]").close();

      Thread.sleep(3000);
      result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(8, result.stream().count());
      result.close();
    }
  }

  @Test
  public void testUdpateChangedDeleted() throws InterruptedException {
    try (ODatabaseSession db = orientDB.open(this.getClass().getSimpleName(), "admin", "admpwd")) {
      String className = "testUpdateDeleteClass";
      String viewName = "testUpdateDelete";
      db.createClass(className);

      for (int i = 0; i < 10; i++) {
        OElement elem = db.newElement(className);
        elem.setProperty("name", "name" + i);
        elem.setProperty("surname", "surname" + i);
        elem.setProperty("toChange", "data");
        elem.save();
      }

      String statement =
          "CREATE VIEW "
              + viewName
              + " FROM (SELECT FROM "
              + className
              + " where toChange=\"data\") metadata {\"updateIntervalSeconds\":1} ";

      db.command(statement);

      Thread.sleep(2000);

      OResultSet result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(10, result.stream().count());
      result.close();

      db.command(
              "update " + className + " set toChange=\"other\" where name in [\"name2\",\"name6\"]")
          .close();

      Thread.sleep(3000);
      result = db.query("SELECT FROM " + viewName);
      Assert.assertEquals(8, result.stream().count());
      result.close();
    }
  }

  @After
  public void after() {
    orientDB.close();
  }
}
