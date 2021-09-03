package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
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
    db.command(new OCommandSQL("CREATE CLASS Item extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Item.name STRING")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Item.map EMBEDDEDMAP")).execute();
  }

  @Override
  protected void executeTest() throws Exception {
    OrientDB orientDB = serverInstance.get(0).server.getContext();
    ODatabaseDocument db = orientDB.open(getDatabaseName(), "admin", "admin");

    try {
      db.command(new OCommandSQL("INSERT into Item (name) values ('foo')")).execute();

      Iterable<ODocument> result =
          db.command(new OCommandSQL("select set(name) as names from Item")).execute();
      Assert.assertEquals(Collections.singleton("foo"), result.iterator().next().field("names"));

      result = db.command(new OCommandSQL("select list(name) as names from Item")).execute();
      Assert.assertEquals(
          Collections.singletonList("foo"), result.iterator().next().field("names"));

      db.command(new OCommandSQL("INSERT into Item (map) values ({'a':'b'}) return @this"))
          .execute();

      result = db.command(new OCommandSQL("select map(map) as names from Item")).execute();
      Assert.assertEquals(
          Collections.singletonMap("a", "b"), result.iterator().next().field("names"));

    } finally {
      db.close();
    }
  }
}
