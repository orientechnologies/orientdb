package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PredicateSecurityTest {
  private static String DB_NAME = PredicateSecurityTest.class.getSimpleName();
  private static OrientDB orient;
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
            + "' role admin, reader identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role reader, writer identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role writer)");
    this.db = orient.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @After
  public void after() {
    this.db.close();
    orient.drop(DB_NAME);
    this.db = null;
  }

  @Test
  public void testCreate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setCreateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);
    try {
      elem = db.newElement("Person");
      elem.setProperty("name", "bar");
      db.save(elem);
      Assert.fail();
    } catch (OSecurityException ex) {
    }
  }

  @Test
  public void testSqlCreate() throws InterruptedException {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setCreateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    Thread.sleep(500);
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    db.command("insert into Person SET name = 'foo'");
    try {
      db.command("insert into Person SET name = 'bar'");
      Assert.fail();
    } catch (OSecurityException ex) {
    }
  }

  @Test
  public void testSqlRead() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select from Person where name = 'bar'");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlReadWithIndex2() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("surname = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertEquals("foo", item.getProperty("surname"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBeforeUpdateCreate() throws InterruptedException {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setBeforeUpdateRule("name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    Thread.sleep(500);
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);
    try {
      elem.setProperty("name", "baz");
      db.save(elem);
      Assert.fail();
    } catch (OSecurityException ex) {
    }

    elem = elem.reload(null, true, true);
    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testBeforeUpdateCreateSQL() throws InterruptedException {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setBeforeUpdateRule("name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();

    if (!doTestBeforeUpdateSQL()) {
      db.close();
      Thread.sleep(500);
      if (!doTestBeforeUpdateSQL()) {
        Assert.fail();
      }
    }
  }

  private boolean doTestBeforeUpdateSQL() {
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);
    try {
      db.command("update Person set name = 'bar'");
      return false;
    } catch (OSecurityException ex) {
    }

    elem = elem.reload(null, true, true);
    Assert.assertEquals("foo", elem.getProperty("name"));
    return true;
  }

  @Test
  public void testAfterUpdate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setAfterUpdateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);
    try {
      elem.setProperty("name", "bar");
      db.save(elem);
      Assert.fail();
    } catch (OSecurityException ex) {
    }

    elem = elem.reload(null, true, true);
    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testAfterUpdateSQL() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setAfterUpdateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);
    try {
      db.command("update Person set name = 'bar'");
      Assert.fail();
    } catch (OSecurityException ex) {
    }

    elem = elem.reload(null, true, true);
    Assert.assertEquals("foo", elem.getProperty("name"));
  }

  @Test
  public void testDelete() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setDeleteRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);
    try {
      db.delete(elem);
      Assert.fail();
    } catch (OSecurityException ex) {
    }

    elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);
    db.delete(elem);
  }

  @Test
  public void testDeleteSQL() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setDeleteRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "writer"

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.command("delete from Person where name = 'foo'");
    try {
      db.command("delete from Person where name = 'bar'");
      Assert.fail();
    } catch (OSecurityException ex) {
    }

    OResultSet rs = db.query("select from Person");
    Assert.assertTrue(rs.hasNext());
    Assert.assertEquals("bar", rs.next().getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testSqlCount() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select count(*) as count from Person");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testSqlCountWithIndex() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"
    OResultSet rs = db.query("select count(*) as count from Person where name = 'bar'");
    Assert.assertEquals(0L, (long) rs.next().getProperty("count"));
    rs.close();

    rs = db.query("select count(*) as count from Person where name = 'foo'");
    Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
    rs.close();
  }

  @Test
  public void testIndexGet() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
    db.command("create index Person.name on Person (name) NOTUNIQUE");

    OSecurityPolicyImpl policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setReadRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "reader"), "database.class.Person", policy);

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    this.db = orient.open(DB_NAME, "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD); // "reader"

    OIndex index = db.getMetadata().getIndexManager().getIndex("Person.name");

    try (Stream<ORID> rids = index.getInternal().getRids("bar")) {
      Assert.assertEquals(0, rids.count());
    }

    try (Stream<ORID> rids = index.getInternal().getRids("foo")) {
      Assert.assertEquals(1, rids.count());
    }
  }
}
