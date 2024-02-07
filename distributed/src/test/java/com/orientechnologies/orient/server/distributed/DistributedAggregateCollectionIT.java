package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class DistributedAggregateCollectionIT extends AbstractServerClusterTest {
  private static final int SERVERS = 1;

  @Override
  public String getDatabaseName() {
    return "DistributedAggregateCollectionIT";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterDatabaseCreation(ODatabaseDocument db) {
    db.command("CREATE CLASS Item extends V").close();
    db.command("CREATE PROPERTY Item.name STRING").close();
    db.command("CREATE PROPERTY Item.map EMBEDDEDMAP").close();
  }

  @Override
  protected void executeTest() throws Exception {
    OrientDB orientDB = serverInstance.get(0).getServerInstance().getContext();
    ODatabaseDocument db = orientDB.open(getDatabaseName(), "admin", "admin");

    try {
      db.command("INSERT into Item (name) values ('foo')").close();

      OResultSet result = db.query("select set(name) as names from Item");
      Assert.assertEquals(Collections.singleton("foo"), result.next().getProperty("names"));

      result = db.query("select list(name) as names from Item");
      Assert.assertEquals(Collections.singletonList("foo"), result.next().getProperty("names"));

      db.command("INSERT into Item (map) values ({'a':'b'}) return @this").close();

      result = db.query("select map(map) as names from Item");
      Assert.assertEquals(Collections.singletonMap("a", "b"), result.next().getProperty("names"));

    } finally {
      db.close();
    }
  }
}
