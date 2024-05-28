package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class OLiveQueryShotdownTest {

  private OServer server;

  public void bootServer() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    OrientDB orientdb =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientdb.execute(
        "create database "
            + OLiveQueryShotdownTest.class.getSimpleName()
            + " memory users(admin identified by 'adminpwd' role admin)");
    orientdb.close();
  }

  public void shutdownServer() {
    server.shutdown();
    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Orient.instance().startup();
  }

  @Test
  public void testShutDown() throws Exception {
    bootServer();
    OrientDB orientdb = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
    ODatabaseDocument db =
        orientdb.open(OLiveQueryShotdownTest.class.getSimpleName(), "admin", "adminpwd");
    db.getMetadata().getSchema().createClass("Test");
    final CountDownLatch error = new CountDownLatch(1);
    try {
      db.command(
              new OLiveQuery(
                  "live select from Test",
                  new OLiveResultListener() {

                    @Override
                    public void onUnsubscribe(int iLiveToken) {}

                    @Override
                    public void onLiveResult(int iLiveToken, ORecordOperation iOp)
                        throws OException {}

                    @Override
                    public void onError(int iLiveToken) {
                      error.countDown();
                    }
                  }))
          .execute();

      shutdownServer();

      assertTrue("onError method never called on shutdow", error.await(2, TimeUnit.SECONDS));

    } finally {
      //      db.close();
      orientdb.close();
    }
  }
}
