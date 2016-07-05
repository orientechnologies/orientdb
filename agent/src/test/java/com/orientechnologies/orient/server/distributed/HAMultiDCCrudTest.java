package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class HAMultiDCCrudTest extends AbstractServerClusterTest {
  private final static int SERVERS = 3;

  @Override
  public String getDatabaseName() {
    return "HAMultiDCCrudTest";
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
    final ODistributedConfiguration cfg = serverInstance.get(0).getServerInstance().getDistributedManager()
        .getDatabaseConfiguration(getDatabaseName());

    Assert.assertTrue(cfg.hasDataCenterConfiguration());
    Assert.assertTrue(cfg.isLocalDataCenterWriteQuorum());

    Assert.assertEquals(cfg.getDataCenterOfServer("europe-0"), "rome");
    Assert.assertEquals(cfg.getDataCenterOfServer("europe-1"), "rome");
    Assert.assertEquals(cfg.getDataCenterOfServer("europe-2"), "rome");
    Assert.assertEquals(cfg.getDataCenterOfServer("usa-0"), "austin");
    Assert.assertEquals(cfg.getDataCenterOfServer("usa-1"), "austin");
    Assert.assertEquals(cfg.getDataCenterOfServer("usa-2"), "austin");

    final List<String> romeDc = cfg.getDataCenterServers("rome");
    Assert.assertEquals(romeDc.size(), 3);
    Assert.assertTrue(romeDc.contains("europe-0"));
    Assert.assertTrue(romeDc.contains("europe-1"));
    Assert.assertTrue(romeDc.contains("europe-2"));

    final List<String> austinDc = cfg.getDataCenterServers("austin");
    Assert.assertEquals(austinDc.size(), 3);
    Assert.assertTrue(austinDc.contains("usa-0"));
    Assert.assertTrue(austinDc.contains("usa-1"));
    Assert.assertTrue(austinDc.contains("usa-2"));

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:target/server0/databases/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      db.command(new OCommandSQL("INSERT into Item (name) values ('foo')")).execute();
    } finally {
      db.close();
    }

    db = new ODatabaseDocumentTx("plocal:target/server1/databases/" + getDatabaseName());
    db.open("admin", "admin");
    try {
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

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }
}
