package com.orientechnologies;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public class BaseMemoryInternalDatabase {

  protected ODatabaseDocumentInternal db;
  protected OrientDB context;
  @Rule public TestName name = new TestName();

  @Before
  public void beforeTest() {
    context = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    context
        .execute(
            "create database "
                + getDatabaseName()
                + " memory users(admin identified by 'adminpwd' role admin) ")
        .close();
    db = (ODatabaseDocumentInternal) context.open(getDatabaseName(), "admin", "adminpwd");
  }

  protected String getDatabaseName() {
    return name.getMethodName();
  }

  @After
  public void afterTest() {
    db.close();
    context.drop(getDatabaseName());
    context.close();
  }
}
