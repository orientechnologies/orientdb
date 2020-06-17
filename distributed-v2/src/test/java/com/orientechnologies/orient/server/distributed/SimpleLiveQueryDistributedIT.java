package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OServer;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleLiveQueryDistributedIT {

  private OServer server0;
  private OServer server1;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create(SimpleLiveQueryDistributedIT.class.getSimpleName(), ODatabaseType.PLOCAL);
    ODatabaseSession session =
        remote.open(SimpleLiveQueryDistributedIT.class.getSimpleName(), "admin", "admin");
    session.createClass("test");
    session.close();
    remote.close();
  }

  @Test
  public void testLiveQueryDifferentNode() throws InterruptedException {
    OrientDB remote1 =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session =
        remote1.open(SimpleLiveQueryDistributedIT.class.getSimpleName(), "admin", "admin");

    EventListener listener = new EventListener();
    OLiveQueryMonitor monitor = session.live("select from test", listener);

    OrientDB remote2 =
        new OrientDB("remote:localhost:2425", "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session2 =
        remote1.open(SimpleLiveQueryDistributedIT.class.getSimpleName(), "admin", "admin");
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
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop(SimpleLiveQueryDistributedIT.class.getSimpleName());
    remote.close();

    server0.shutdown();
    server1.shutdown();
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
