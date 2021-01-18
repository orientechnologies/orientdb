package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.FetchFromIndexStep;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ColumnSecurityTest {
  static String DB_NAME = "test";
  static OrientDB orient;
  private ODatabaseSession db;

  @BeforeClass
  public static void beforeClass() {
    orient =
        new OrientDB(
            "plocal:.",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
  }

  @AfterClass
  public static void afterClass() {
    orient.close();
  }

  @Before
  public void before() {
    orient.execute(
        "create database "
            + DB_NAME
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    this.db = orient.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop("test");
    this.db = null;
  }

  @Test
  public void testIndexWithPolicy() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy1() throws InterruptedException {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
    person.createProperty("surname", OType.STRING);

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    Thread.sleep(100);
    try {
      db.command("create index Person.name_surname on Person (name, surname) NOTUNIQUE");
      Assert.fail();
    } catch (OIndexException e) {

    }
  }

  @Test
  public void testIndexWithPolicy2() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setCreateRule("name = 'foo'");
    policy.setBeforeUpdateRule("name = 'foo'");
    policy.setAfterUpdateRule("name = 'foo'");
    policy.setDeleteRule("name = 'foo'");

    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy3() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.surname", policy);

    db.command("create index Person.name on Person (name) NOTUNIQUE");
  }

  @Test
  public void testIndexWithPolicy4() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
    person.createProperty("address", OType.STRING);

    db.command("create index Person.name_address on Person (name, address) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.surname", policy);
  }

  @Test
  public void testIndexWithPolicy5() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
    person.createProperty("surname", OType.STRING);

    db.command("create index Person.name_surname on Person (name, surname) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);

    try {
      security.setSecurityPolicy(
          db, security.getRole(db, "reader"), "database.class.Person.name", policy);
      Assert.fail();
    } catch (Exception e) {
    }
  }

  @Test
  public void testIndexWithPolicy6() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);

    db.command("create index Person.name on Person (name) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);
  }

  @Test
  public void testReadFilterOneProperty() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", "reader");
    OResultSet rs = db.query("select from Person");
    boolean fooFound = false;
    boolean nullFound = false;

    for (int i = 0; i < 2; i++) {
      OResult item = rs.next();
      if ("foo".equals(item.getProperty("name"))) {
        fooFound = true;
      }
      if (item.getProperty("name") == null) {
        nullFound = true;
      }
    }

    Assert.assertFalse(rs.hasNext());
    rs.close();

    Assert.assertTrue(fooFound);
    Assert.assertTrue(nullFound);
  }

  @Test
  public void testReadFilterOnePropertyWithIndex() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass clazz = db.createClass("Person");
    clazz.createProperty("name", OType.STRING);

    db.command("create index Person.name on Person (name) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", "reader");
    OResultSet rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    rs = db.query("select from Person where name = 'bar'");
    Assert.assertFalse(rs.hasNext());
    Assert.assertTrue(
        rs.getExecutionPlan().get().getSteps().stream()
            .anyMatch(x -> x instanceof FetchFromIndexStep));
    rs.close();
  }

  @Test
  public void testReadWithPredicateAndQuery() throws InterruptedException {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name IN (select 'foo' as foo)");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    OResultSet rs = db.query("select from Person");
    boolean fooFound = false;
    boolean barFound = false;

    for (int i = 0; i < 2; i++) {
      OResult item = rs.next();
      if ("foo".equals(item.getProperty("name"))) {
        fooFound = true;
      }
      if ("bar".equals(item.getProperty("name"))) {
        barFound = true;
      }
    }

    Assert.assertFalse(rs.hasNext());
    rs.close();

    Assert.assertTrue(fooFound);
    Assert.assertTrue(barFound);

    db.close();
    Thread.sleep(200);
    this.db = orient.open(DB_NAME, "reader", "reader");
    rs = db.query("select from Person");
    fooFound = false;
    boolean nullFound = false;

    for (int i = 0; i < 2; i++) {
      OResult item = rs.next();
      if ("foo".equals(item.getProperty("name"))) {
        fooFound = true;
      }
      if (item.getProperty("name") == null) {
        nullFound = true;
      }
    }

    Assert.assertFalse(rs.hasNext());
    rs.close();

    Assert.assertTrue(fooFound);
    Assert.assertTrue(nullFound);
  }

  @Test
  public void testReadFilterOnePropertyWithQuery() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "reader"), "database.class.Person.name", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", "reader");
    OResultSet rs = db.query("select from Person where name = 'foo' OR name = 'bar'");

    OResult item = rs.next();
    Assert.assertEquals("foo", item.getProperty("name"));

    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testCreate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setCreateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");
    try {
      db.save(elem);
      Assert.fail();
    } catch (OSecurityException e) {

    }
  }

  @Test
  public void testBeforeUpdate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setBeforeUpdateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    elem.setProperty("surname", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

    db.command("UPDATE Person SET name = 'foo1' WHERE name = 'foo'");

    try (OResultSet rs = db.query("SELECT FROM Person WHERE name = 'foo1'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }

    try {
      db.command("UPDATE Person SET name = 'bar1' WHERE name = 'bar'");
      Assert.fail();
    } catch (OSecurityException e) {

    }
  }

  @Test
  public void testAfterUpdate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setAfterUpdateRule("name <> 'invalid'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(
        db, security.getRole(db, "writer"), "database.class.Person.name", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

    db.command("UPDATE Person SET name = 'foo1' WHERE name = 'foo'");

    try (OResultSet rs = db.query("SELECT FROM Person WHERE name = 'foo1'")) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
      Assert.assertFalse(rs.hasNext());
    }

    try {
      db.command("UPDATE Person SET name = 'invalid'");
      Assert.fail();
    } catch (OSecurityException e) {

    }
  }

  @Test
  public void testReadHiddenColumn() {
    db.command("CREATE CLASS Person");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'bar')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    db.close();

    db = orient.open(DB_NAME, "reader", "reader");
    try (final OResultSet resultSet = db.query("SELECT from Person")) {
      OResult item = resultSet.next();
      Assert.assertNull(item.getProperty("name"));
    }
  }

  @Test
  public void testUpdateHiddenColumn() {

    db.command("CREATE CLASS Person");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'bar')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    db.close();

    db = orient.open(DB_NAME, "reader", "reader");
    try (final OResultSet resultSet = db.query("SELECT from Person")) {
      OResult item = resultSet.next();
      Assert.assertNull(item.getProperty("name"));
      OElement doc = item.getElement().get();
      doc.setProperty("name", "bar");
      try {
        doc.save();
        Assert.fail();
      } catch (Exception e) {

      }
    }
  }
}
