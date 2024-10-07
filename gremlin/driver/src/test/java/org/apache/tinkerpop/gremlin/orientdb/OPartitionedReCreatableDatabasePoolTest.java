package org.apache.tinkerpop.gremlin.orientdb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class OPartitionedReCreatableDatabasePoolTest {

  protected OPartitionedReCreatableDatabasePool pool() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    String dbName = "memorydb" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);

    orientDB.execute(
        "create database " + dbName + " memory users(admin identified by 'admin' role admin)");

    return new OPartitionedReCreatableDatabasePool(orientDB, dbName, "admin", "admin", 5);
  }

  @Test
  public void testDatabaseAcquiredByOPartitionedReCreatableDatabasePool() throws Exception {
    OPartitionedReCreatableDatabasePool pool = pool();
    assertFalse(pool.acquire().isClosed());

    pool.close();
    assertNull(pool.acquire());

    pool.reCreatePool();
    assertFalse(pool.acquire().isClosed());
  }
}
