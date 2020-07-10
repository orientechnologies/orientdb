package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class HAMultiDCCrudTest extends AbstractServerClusterTest {
  private static final int SERVERS = 3;

  @Override
  public String getDatabaseName() {
    return "HAMultiDCCrudTest";
  }

  @Test
  @Ignore
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
    OGlobalConfiguration.NETWORK_SOCKET_RETRY_STRATEGY.setValue("same-dc");
    final ODistributedConfiguration cfg =
        serverInstance
            .get(0)
            .getServerInstance()
            .getDistributedManager()
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

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost:2424/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      db.command(new OCommandSQL("INSERT into Item (name) values ('foo')")).execute();
    } finally {
      db.close();
    }

    db = new ODatabaseDocumentTx("remote:localhost:2425/" + getDatabaseName());
    db.open("admin", "admin");
    try {
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

    // TRY AN INSERT AGAINST THE DC WITHOUT QUORUM EXPECTING TO FAIL
    db = new ODatabaseDocumentTx("remote:localhost:2426/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      db.command(new OCommandSQL("INSERT into Item (map) values ({'a':'b'}) return @this"))
          .execute();
      Assert.fail("Quorum not reached, but no failure has been caught");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause().toString().contains("Quorum"));
    } finally {
      db.close();
    }

    // KILL ONE SERVER TO CHECK IF QUORUM FAILS
    serverInstance.get(0).getServerInstance().shutdown();

    // RETRY AN INSERT AND CHECK IT FAILS (NO QUORUM)
    db = new ODatabaseDocumentTx("remote:localhost:2425/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      db.command(new OCommandSQL("INSERT into Item (map) values ({'a':'b'}) return @this"))
          .execute();
      Assert.fail("Quorum not reached, but no failure has been caught");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause().toString().contains("Quorum"));
    } finally {
      db.close();
    }

    // RESTART THE FIRST SERVER DOWN
    serverInstance.get(0).getServerInstance().restart();

    waitForDatabaseIsOnline(
        serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName(),
        getDatabaseName(),
        30000);

    // RETRY AN INSERT AND CHECK IT DOESN'T FAILS (QUORUM REACHED)
    db = new ODatabaseDocumentTx("remote:localhost:2425/" + getDatabaseName());
    db.open("admin", "admin");
    try {
      db.command(new OCommandSQL("INSERT into Item (map) values ({'a':'b'}) return @this"))
          .execute();
    } finally {
      db.close();
    }
  }

  @Override
  protected String getDistributedServerConfiguration(ServerRun server) {
    return "dc-server-config-" + server.getServerId() + ".xml";
  }
}
