package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OClassSecurityTest {
  private static String DB_NAME = PredicateSecurityTest.class.getSimpleName();
  private static OrientDB orient;
  private ODatabaseSession db;

  @BeforeClass
  public static void beforeClass() {
    orient = new OrientDB("plocal:.", OrientDBConfig.defaultConfig());
  }

  @AfterClass
  public static void afterClass() {
    orient.close();
  }

  @Before
  public void before() {
    orient.create(DB_NAME, ODatabaseType.MEMORY);
    this.db = orient.open(DB_NAME, "admin", "admin");
  }

  @After
  public void after() {
    this.db.close();
    orient.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testReadWithClassPermissions() {
    db.createClass("Person");
    ORole reader = db.getMetadata().getSecurity().getRole("reader");
    reader.grant(ORule.ResourceGeneric.CLASS, "Person", ORole.PERMISSION_NONE);
    reader.revoke(ORule.ResourceGeneric.CLASS, "Person", ORole.PERMISSION_READ);
    reader.save();

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    db.close();

    db = orient.open(DB_NAME, "reader", "reader");
    try (final OResultSet resultSet = db.query("SELECT from Person")) {
      Assert.assertFalse(resultSet.hasNext());
    }
  }
}
