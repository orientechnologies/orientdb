package com.orientechnologies.orient.graph.gremlin;

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

public class LocalGraphDbTest {
  private static String DB_URL = "local:target/databases/tinkerpop";

  public static void main(String[] args) throws IOException {
    new LocalGraphDbTest().multipleDatabasesSameThread();
  }

  public LocalGraphDbTest() {
    OGremlinHelper.global().create();
  }

  @BeforeClass
  public void before() {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
  }

  @AfterClass
  public void after() {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(true);
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
