package com.orientechnologies.lucene.test;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.File;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 23/09/16. */
public class LuceneFreezeReleaseTest {

  @Before
  public void setUp() throws Exception {
    OFileUtils.deleteRecursively(new File("./target/freezeRelease"));
  }

  @Test
  public void freezeReleaseTest() {
    if (isWindows()) return;
    try (OrientDB orientdb = new OrientDB("embedded:target/", OrientDBConfig.defaultConfig())) {
      orientdb.execute(
          "create database freezeRelease plocal users(admin identified by 'adminpwd' role"
              + " admin)");

      try (ODatabaseDocument db = orientdb.open("freezeRelease", "admin", "adminpwd")) {

        OSchema schema = db.getMetadata().getSchema();
        OClass person = schema.createClass("Person");
        person.createProperty("name", OType.STRING);

        db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE").close();

        db.save(new ODocument("Person").field("name", "John"));

        OResultSet results = db.query("select from Person where name lucene 'John'");
        Assert.assertEquals(1, results.stream().count());
        db.freeze();

        results = db.query("select from Person where name lucene 'John'");
        Assert.assertEquals(1, results.stream().count());

        db.release();

        db.save(new ODocument("Person").field("name", "John"));

        results = db.query("select from Person where name lucene 'John'");
        Assert.assertEquals(2, results.stream().count());
      }
      orientdb.drop("freezeRelease");
    }
  }

  // With double calling freeze/release
  @Test
  public void freezeReleaseMisUsageTest() {
    if (isWindows()) return;
    try (OrientDB orientdb = new OrientDB("embedded:target/", OrientDBConfig.defaultConfig())) {
      orientdb.execute(
          "create database freezeRelease plocal users(admin identified by 'adminpwd' role"
              + " admin)");

      try (ODatabaseDocument db = orientdb.open("freezeRelease", "admin", "adminpwd")) {

        OSchema schema = db.getMetadata().getSchema();
        OClass person = schema.createClass("Person");
        person.createProperty("name", OType.STRING);

        db.command("create index Person.name on Person (name) FULLTEXT ENGINE LUCENE").close();

        db.save(new ODocument("Person").field("name", "John"));

        OResultSet results = db.query("select from Person where name lucene 'John'");
        Assert.assertEquals(1, results.stream().count());

        db.freeze();

        db.freeze();

        results = db.query("select from Person where name lucene 'John'");
        Assert.assertEquals(1, results.stream().count());

        db.release();
        db.release();

        db.save(new ODocument("Person").field("name", "John"));

        results = db.query("select from Person where name lucene 'John'");
        Assert.assertEquals(2, results.stream().count());
      }
      orientdb.drop("freezeRelease");
    }
  }

  private boolean isWindows() {
    final String osName = System.getProperty("os.name").toLowerCase();
    return osName.contains("win");
  }
}
