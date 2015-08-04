package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Collections;

public class DistributedAggregateCollectionTest extends AbstractServerClusterTest {
  private final static int SERVERS = 1;

  @Override
  public String getDatabaseName() {
    return "DistributedAggregateCollectionTest";
  }

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph db) {
    db.command(new OCommandSQL("CREATE CLASS Item extends V")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Item.name STRING")).execute();
    db.command(new OCommandSQL("CREATE PROPERTY Item.map EMBEDDEDMAP")).execute();
  }

  @Override
  protected void executeTest() throws Exception {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/server0/databases/" + getDatabaseName());
    db.open("admin", "admin");

    try {
      db.command(new OCommandSQL("INSERT into Item (name) values ('foo')")).execute();

      Iterable<ODocument> result = db.command(new OCommandSQL("select set(name) as names from Item")).execute();
      Assert.assertEquals(Collections.singleton("foo"), result.iterator().next().field("names"));

      result = db.command(new OCommandSQL("select list(name) as names from Item")).execute();
      Assert.assertEquals(Collections.singletonList("foo"), result.iterator().next().field("names"));

      db.command(new OCommandSQL("INSERT into Item (map) values ({'a':'b'}) return @this")).execute();

      result = db.command(new OCommandSQL("select map(map) as names from Item")).execute();
      Assert.assertEquals(Collections.singletonMap("a", "b"), result.iterator().next().field("names"));

    } finally {
      db.close();
    }
  }
}
