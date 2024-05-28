package com.orientechnologies.spatial;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.io.File;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by frank on 15/12/2016. */
public class LuceneSpatialDropTest {

  private int insertcount;
  private String dbpath;
  private OrientDB context;

  @Before
  public void setUp() throws Exception {

    dbpath = "plocal:./target/databases/" + this.getClass().getSimpleName();

    // @maggiolo00 set cont to 0 and the test will not fail anymore
    insertcount = 100;
    context = new OrientDB("embedded:./target/databases/", OrientDBConfig.defaultConfig());
    context.execute(
        "create database "
            + this.getClass().getSimpleName()
            + " plocal users(admin identified by 'adminpwd' role admin)");
    ODatabaseDocument db = context.open(this.getClass().getSimpleName(), "admin", "adminpwd");

    OClass test = db.getMetadata().getSchema().createClass("test");
    test.createProperty("name", OType.STRING);
    test.createProperty("latitude", OType.DOUBLE).setMandatory(false);
    test.createProperty("longitude", OType.DOUBLE).setMandatory(false);
    db.command("create index test.name on test (name) FULLTEXT ENGINE LUCENE").close();
    db.command("create index test.ll on test (latitude,longitude) SPATIAL ENGINE LUCENE").close();
    db.close();
  }

  @After
  public void after() {
    context.close();
  }

  @Test
  public void testDeleteLuceneIndex1() {

    ODatabasePool dbPool =
        new ODatabasePool(this.context, this.getClass().getSimpleName(), "admin", "adminpwd");

    ODatabaseDocument db = dbPool.acquire();
    fillDb(db, insertcount);
    db.close();

    db = dbPool.acquire();
    // @maggiolo00 Remove the next three lines and the test will not fail anymore
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>(
            "select from test where [latitude,longitude] WITHIN [[50.0,8.0],[51.0,9.0]]");
    List<ODocument> result = db.command(query).execute();
    Assert.assertEquals(insertcount, result.size());
    db.close();
    dbPool.close();

    // reopen to drop
    context.drop(this.getClass().getSimpleName());
    File dbFolder = new File(dbpath);
    Assert.assertEquals(false, dbFolder.exists());
  }

  private void fillDb(ODatabaseDocument db, int count) {
    for (int i = 0; i < count; i++) {
      ODocument doc = new ODocument("test");
      doc.field("name", "TestInsert" + i);
      doc.field("latitude", 50.0 + (i * 0.000001));
      doc.field("longitude", 8.0 + (i * 0.000001));
      db.save(doc);
    }
    OResultSet result = db.query("select * from test");
    Assert.assertEquals(count, result.stream().count());
  }
}
