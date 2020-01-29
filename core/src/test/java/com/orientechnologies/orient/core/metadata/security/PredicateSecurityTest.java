package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.*;

import java.util.stream.Stream;

public class PredicateSecurityTest {

  private static String           DB_NAME = PredicateSecurityTest.class.getSimpleName();
  private static OrientDB         orient;
  private        ODatabaseSession db;

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
  public void testCreate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setCreateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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
  public void testSqlCreate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setCreateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
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
    this.db = orient.open(DB_NAME, "reader", "reader");
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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
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
    this.db = orient.open(DB_NAME, "reader", "reader");
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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
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
    this.db = orient.open(DB_NAME, "reader", "reader");
    OResultSet rs = db.query("select from Person where name = 'foo'");
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertEquals("foo", item.getProperty("surname"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBeforeUpdateCreate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setBeforeUpdateRule("name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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
  public void testBeforeUpdateCreateSQL() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setBeforeUpdateRule("name = 'bar'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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
  public void testAfterUpdate() {
    OSecurityInternal security = ((ODatabaseInternal) db).getSharedContext().getSecurity();

    db.createClass("Person");

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setAfterUpdateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setAfterUpdateRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setDeleteRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
    policy.setActive(true);
    policy.setDeleteRule("name = 'foo'");
    security.saveSecurityPolicy(db, policy);
    security.setSecurityPolicy(db, security.getRole(db, "writer"), "database.class.Person", policy);

    db.close();
    this.db = orient.open(DB_NAME, "writer", "writer");

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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
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
    this.db = orient.open(DB_NAME, "reader", "reader");
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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
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
    this.db = orient.open(DB_NAME, "reader", "reader");
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

    OSecurityPolicy policy = security.createSecurityPolicy(db, "testPolicy");
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
    this.db = orient.open(DB_NAME, "reader", "reader");

    OIndex index = db.getMetadata().getIndexManager().getIndex("Person.name");

    try (Stream<ORID> rids = index.getInternal().getRids("bar")) {
      Assert.assertEquals(0, rids.count());
    }

    try (Stream<ORID> rids = index.getInternal().getRids("foo")) {
      Assert.assertEquals(1, rids.count());
    }
  }
}
