package com.orientechnologies.lucene.test;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * Created by frank on 15/12/2016.
 */
public class LuceneDropTest {

  private int    insertcount;
  private String dbName;

  @Before
  public void setUp() throws Exception {

    dbName = "plocal:./target/databases/" + this.getClass().getSimpleName();

    // @maggiolo00 set cont to 0 and the test will not fail anymore
    insertcount = 100;

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbName);

    db.create();
    OClass test = db.getMetadata().getSchema().createClass("test");
    test.createProperty("name", OType.STRING);
    db.command(new OCommandSQL("create index test.name on test (name) FULLTEXT ENGINE LUCENE")).execute();
    db.close();

  }

  @After
  public void tearDown() throws Exception {
    OFileUtils.deleteRecursively(new File(dbName));

  }

  @Test
  public void testDeleteLuceneIndex() {

    OPartitionedDatabasePool dbPool = new OPartitionedDatabasePool(dbName, "admin", "admin");

    ODatabaseDocumentTx db = dbPool.acquire();
    fillDb(db, insertcount);
    db.close();

    db = dbPool.acquire();
    // @maggiolo00 Remove the next three lines and the test will not fail anymore
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(
        "select from test ");
    List<ODocument> result = db.command(query).execute();
    Assert.assertEquals(insertcount, result.size());
    db.close();
    dbPool.close();

    //reopen to drop
    db = new ODatabaseDocumentTx(dbName).open("admin", "admin");

    db.drop();

    File dbFolder = new File("./target/databases/" + this.getClass().getSimpleName());
    Assert.assertEquals(false, dbFolder.exists());

  }

  private void fillDb(ODatabaseDocumentTx db, int count) {
    for (int i = 0; i < count; i++) {
      ODocument doc = new ODocument("test");
      doc.field("name", "Test" + i);
      db.save(doc);
    }
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select * from test");
    List<ODocument> result = db.command(query).execute();
    Assert.assertEquals(count, result.size());
  }
}
