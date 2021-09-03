package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleLiveQueryDistributedIT {

  private TestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;
  private String databaseName = SimpleLiveQueryDistributedIT.class.getSimpleName();

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = TestSetupUtil.create(config);
    setup.setup();

    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", databaseName);
    ODatabaseSession session = remote.open(databaseName, "admin", "admin");
    session.createClass("test");
    session.close();
    remote.close();
  }

  @Test
  public void testLiveQueryDifferentNode() throws InterruptedException {
    OrientDB remote1 = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session = remote1.open(databaseName, "admin", "admin");

    EventListener listener = new EventListener();
    OLiveQueryMonitor monitor = session.live("select from test", listener);

    OrientDB remote2 = setup.createRemote(server1, "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session2 = remote1.open(databaseName, "admin", "admin");
    OElement el = session2.save(session2.newElement("test"));
    el.setProperty("name", "name");
    session2.save(el);
    session2.delete(el);
    session2.close();

    session.activateOnCurrentThread();
    monitor.unSubscribe();
    session.close();
    listener.latch.await();
    assertEquals(1, listener.create);
    assertEquals(1, listener.delete);
    assertEquals(1, listener.update);
    remote1.close();
    remote2.close();
  }

  @After
  public void after() throws InterruptedException {
    setup.teardown();
    ODatabaseDocumentTx.closeAll();
  }

  private static class EventListener implements OLiveQueryResultListener {
    public int create = 0;
    public int update = 0;
    public int delete = 0;
    public CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void onCreate(ODatabaseDocument database, OResult data) {
      create++;
    }

    @Override
    public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
      update++;
    }

    @Override
    public void onDelete(ODatabaseDocument database, OResult data) {
      delete++;
    }

    @Override
    public void onError(ODatabaseDocument database, OException exception) {}

    @Override
    public void onEnd(ODatabaseDocument database) {
      latch.countDown();
    }
  }
}
