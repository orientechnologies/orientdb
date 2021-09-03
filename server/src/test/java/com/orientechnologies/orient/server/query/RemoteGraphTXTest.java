package com.orientechnologies.orient.server.query;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by wolf4ood on 1/03/19. */
public class RemoteGraphTXTest {

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
        RemoteGraphTXTest.class.getSimpleName());
    session = orientDB.open(RemoteGraphTXTest.class.getSimpleName(), "admin", "admin");
    session.createClassIfNotExist("FirstV", "V");
    session.createClassIfNotExist("SecondV", "V");
    session.createClassIfNotExist("TestEdge", "E");
  }

  @Test
  public void itShouldDeleteEdgesInTx() {

    session.command("create vertex FirstV set id = '1'").close();
    session.command("create vertex SecondV set id = '2'").close();
    try (OResultSet resultSet =
        session.command(
            "create edge TestEdge  from ( select from FirstV where id = '1') to ( select from SecondV where id = '2')")) {
      OResult result = resultSet.stream().iterator().next();

      Assert.assertEquals(true, result.isEdge());
    }

    session.begin();

    session
        .command(
            "delete edge TestEdge from (select from FirstV where id = :param1) to (select from SecondV where id = :param2)",
            new HashMap() {
              {
                put("param1", "1");
                put("param2", "2");
              }
            })
        .stream()
        .collect(Collectors.toList());

    session.commit();

    Assert.assertEquals(0, session.query("select from TestEdge").stream().count());

    List<OResult> results =
        session.query("select bothE().size() as count from V").stream()
            .collect(Collectors.toList());

    for (OResult result : results) {
      Assert.assertEquals(0, (int) result.getProperty("count"));
    }
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
