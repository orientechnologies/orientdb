package com.orientechnologies.orient.server.query;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RemoteGraphLiveQueryTest {

  private static final String SERVER_DIRECTORY = "./target/remoteGraph";
  private OServer server;
  private OrientDB orientDB;
  private ODatabaseDocument session;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));

    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        RemoteGraphLiveQueryTest.class.getSimpleName());
    session = orientDB.open(RemoteGraphLiveQueryTest.class.getSimpleName(), "admin", "admin");
    session.createClassIfNotExist("FirstV", "V");
    session.createClassIfNotExist("SecondV", "V");
    session.createClassIfNotExist("TestEdge", "E");
  }

  @Test
  public void testLiveQuery() throws InterruptedException {

    session.command("create vertex FirstV set id = '1'").close();
    session.command("create vertex SecondV set id = '2'").close();
    try (OResultSet resultSet =
        session.command(
            "create edge TestEdge  from (select from FirstV) to (select from SecondV)")) {
      OResult result = resultSet.stream().iterator().next();

      Assert.assertEquals(true, result.isEdge());
    }

    AtomicLong l = new AtomicLong(0);

    session.live(
        "select from SecondV",
        new OLiveQueryResultListener() {

          @Override
          public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
            l.incrementAndGet();
          }

          @Override
          public void onError(ODatabaseDocument database, OException exception) {}

          @Override
          public void onEnd(ODatabaseDocument database) {}

          @Override
          public void onDelete(ODatabaseDocument database, OResult data) {}

          @Override
          public void onCreate(ODatabaseDocument database, OResult data) {}
        },
        new HashMap<String, String>());

    session.command("update SecondV set id = 3");

    Thread.sleep(100);

    Assert.assertEquals(1L, l.get());
  }

  @After
  public void after() {
    session.close();
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }
}
