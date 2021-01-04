package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ORemoteSecurityTests {

  private static String DB_NAME = ORemoteSecurityTests.class.getSimpleName();
  private OrientDB orient;
  private OServer server;
  private ODatabaseSession db;

  @Before
  public void before()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
    server = OServer.startFromClasspathConfig("abstract-orientdb-server-config.xml");
    orient = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orient.execute(
        "create database ? memory users (admin identified by 'admin' role admin, writer identified by 'writer' role writer, reader identified by 'reader' role reader)",
        DB_NAME);
    this.db = orient.open(DB_NAME, "admin", "admin");
    OClass person = db.createClass("Person");
    person.createProperty("name", OType.STRING);
  }

  @After
  public void after() {
    this.db.activateOnCurrentThread();
    this.db.close();
    orient.drop(DB_NAME);
    orient.close();
    server.shutdown();
  }

  @Test
  public void testCreate() {
    db.command("CREATE SECURITY POLICY testPolicy SET create = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      OElement elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      try {
        elem = filteredSession.newElement("Person");
        elem.setProperty("name", "bar");
        filteredSession.save(elem);
        Assert.fail();
      } catch (OSecurityException ex) {
      }
    }
  }

  @Test
  public void testSqlCreate() {
    db.command("CREATE SECURITY POLICY testPolicy SET create = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");
    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      filteredSession.command("insert into Person SET name = 'foo'");
      try {
        filteredSession.command("insert into Person SET name = 'bar'");
        Assert.fail();
      } catch (OSecurityException ex) {
      }
    }
  }

  @Test
  public void testSqlRead() {
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select from Person")) {
        Assert.assertTrue(rs.hasNext());
        rs.next();
        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testSqlReadWithIndex() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");
    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select from Person where name = 'bar'")) {

        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testSqlReadWithIndex2() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (surname = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    elem.setProperty("surname", "bar");
    db.save(elem);

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select from Person where name = 'foo'")) {
        Assert.assertTrue(rs.hasNext());
        OResult item = rs.next();
        Assert.assertEquals("foo", item.getProperty("surname"));
        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testBeforeUpdateCreate() {
    db.command("CREATE SECURITY POLICY testPolicy SET BEFORE UPDATE = (name = 'bar')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      OElement elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      try {
        elem.setProperty("name", "baz");
        filteredSession.save(elem);
        Assert.fail();
      } catch (OSecurityException ex) {
      }
      elem = elem.reload(null, true, true);
      Assert.assertEquals("foo", elem.getProperty("name"));
    }
  }

  @Test
  public void testBeforeUpdateCreateSQL() {
    db.command("CREATE SECURITY POLICY testPolicy SET BEFORE UPDATE = (name = 'bar')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      OElement elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      try {
        filteredSession.command("update Person set name = 'bar'");
        Assert.fail();
      } catch (OSecurityException ex) {
      }

      elem = elem.reload(null, true, true);
      Assert.assertEquals("foo", elem.getProperty("name"));
    }
  }

  @Test
  public void testAfterUpdate() {
    db.command("CREATE SECURITY POLICY testPolicy SET AFTER UPDATE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      OElement elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      try {
        elem.setProperty("name", "bar");
        filteredSession.save(elem);
        Assert.fail();
      } catch (OSecurityException ex) {
      }

      elem = elem.reload(null, true, true);
      Assert.assertEquals("foo", elem.getProperty("name"));
    }
  }

  @Test
  public void testAfterUpdateSQL() {
    db.command("CREATE SECURITY POLICY testPolicy SET AFTER UPDATE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {
      OElement elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      try {
        filteredSession.command("update Person set name = 'bar'");
        Assert.fail();
      } catch (OSecurityException ex) {
      }

      elem = elem.reload(null, true, true);
      Assert.assertEquals("foo", elem.getProperty("name"));
    }
  }

  @Test
  public void testDelete() {
    db.command("CREATE SECURITY POLICY testPolicy SET DELETE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {

      OElement elem = filteredSession.newElement("Person");
      elem.setProperty("name", "bar");
      filteredSession.save(elem);
      try {
        filteredSession.delete(elem);
        Assert.fail();
      } catch (OSecurityException ex) {
      }

      elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);
      filteredSession.delete(elem);
    }
  }

  @Test
  public void testDeleteSQL() {
    db.command("CREATE SECURITY POLICY testPolicy SET DELETE = (name = 'foo')");
    db.command("ALTER ROLE writer SET POLICY testPolicy ON database.class.Person");

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "writer", "writer")) {

      OElement elem = filteredSession.newElement("Person");
      elem.setProperty("name", "foo");
      filteredSession.save(elem);

      elem = filteredSession.newElement("Person");
      elem.setProperty("name", "bar");
      filteredSession.save(elem);

      filteredSession.command("delete from Person where name = 'foo'");
      try {
        filteredSession.command("delete from Person where name = 'bar'");
        Assert.fail();
      } catch (OSecurityException ex) {
      }

      try (OResultSet rs = filteredSession.query("select from Person")) {
        Assert.assertTrue(rs.hasNext());
        Assert.assertEquals("bar", rs.next().getProperty("name"));
        Assert.assertFalse(rs.hasNext());
      }
    }
  }

  @Test
  public void testSqlCount() {
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs = filteredSession.query("select count(*) as count from Person")) {
        Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
      }
    }
  }

  @Test
  public void testSqlCountWithIndex() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    db.close();
    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (OResultSet rs =
          filteredSession.query("select count(*) as count from Person where name = 'bar'")) {
        Assert.assertEquals(0L, (long) rs.next().getProperty("count"));
      }

      try (OResultSet rs =
          filteredSession.query("select count(*) as count from Person where name = 'foo'")) {
        Assert.assertEquals(1L, (long) rs.next().getProperty("count"));
      }
    }
  }

  @Test
  public void testIndexGet() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "bar")) {
        Assert.assertEquals(0, resultSet.stream().count());
      }

      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "foo")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }
    }
  }

  @Test
  public void testIndexGetAndColumnSecurity() {
    db.command("create index Person.name on Person (name) NOTUNIQUE");
    db.command("CREATE SECURITY POLICY testPolicy SET read = (name = 'foo')");
    db.command("ALTER ROLE reader SET POLICY testPolicy ON database.class.Person.name");

    OElement elem = db.newElement("Person");
    elem.setProperty("name", "foo");
    db.save(elem);

    elem = db.newElement("Person");
    elem.setProperty("name", "bar");
    db.save(elem);

    try (ODatabaseSession filteredSession = orient.open(DB_NAME, "reader", "reader")) {
      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "bar")) {
        Assert.assertEquals(0, resultSet.stream().count());
      }

      try (final OResultSet resultSet =
          filteredSession.query("SELECT from Person where name = ?", "foo")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }
    }
  }

  @Test
  public void testReadHiddenColumn() {

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
