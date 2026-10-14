package com.orientechnologies.orient.test.database;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseMemoryDatabase {

  protected ODatabaseSession db;
  protected OrientDB context;
  private String databaseName;

  @BeforeMethod
  public void beforeTest() {
    context = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    String dbName = this.getClass().getSimpleName();
    dbName = dbName.replace('[', '_');
    dbName = dbName.replace(']', '_');
    this.databaseName = dbName;
    context
        .execute(
            "create database "
                + this.databaseName
                + " memory users(admin identified by 'adminpwd' role admin) ")
        .close();
    db = context.open(this.databaseName, "admin", "adminpwd");
  }

  protected void reOpen(String user, String password) {
    this.db.close();
    this.db = context.open(this.databaseName, user, password);
  }

  @AfterMethod
  public void afterTest() {
    db.close();
    context.drop(databaseName);
    context.close();
  }
}
