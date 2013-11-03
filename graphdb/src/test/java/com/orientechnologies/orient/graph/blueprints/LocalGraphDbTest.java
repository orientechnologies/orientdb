package com.orientechnologies.orient.graph.blueprints;

import java.io.IOException;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;

public class LocalGraphDbTest {
  private static String DB_URL = "plocal:target/databases/tinkerpop";

  public static void main(String[] args) throws IOException {
    new LocalGraphDbTest().multipleDatabasesSameThread();
  }

  private boolean oldStorageOpen;

  public LocalGraphDbTest() {
    OGremlinHelper.global().create();
  }

  @BeforeClass
  public void before() {
    oldStorageOpen = OGlobalConfiguration.STORAGE_KEEP_OPEN.getValueAsBoolean();
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
  }

  @AfterClass
  public void after() {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(oldStorageOpen);
  }

  @Test
  public void multipleDatabasesSameThread() throws IOException {
    OGraphDatabase db1 = OGraphDatabasePool.global().acquire(DB_URL, "admin", "admin");
    ODocument doc1 = db1.createVertex();

    doc1.field("key", "value");
    doc1.save();
    db1.close();

    OGraphDatabase db2 = OGraphDatabasePool.global().acquire(DB_URL, "admin", "admin");

    ODocument doc2 = db2.createVertex();
    doc2.field("key", "value");
    doc2.save();
    db2.close();

    db1 = OGraphDatabasePool.global().acquire(DB_URL, "admin", "admin");

    final List<?> result = db1.query(new OSQLSynchQuery<ODocument>("select out[weight=3].size() from V where out.size() > 0"));

    doc1 = db1.createVertex();
    doc1.field("newkey", "newvalue");
    doc1.save();
    db1.close();
  }
}
