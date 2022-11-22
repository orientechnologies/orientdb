package com.orientechnologies.orient.server.distributed.ridbag;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.setup.LocalTestSetup;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RidBagConversionIT {

  private LocalTestSetup setup;
  private SetupConfig config;
  private String server0, server1;

  @Before
  public void before() {
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    setup = new LocalTestSetup(config);
  }

  @Test
  public void testConversion() throws InterruptedException {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(1);
    String server0Path = setup.getServer(server0).getServerHome();
    OrientDB orientDB =
        new OrientDB("embedded:" + server0Path + "/databases/", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", "test");
    ODatabaseSession database = orientDB.open("test", "admin", "admin");
    database.begin();
    OVertex ver = database.newVertex("V");
    OVertex ver1 = database.newVertex("V");
    OVertex ver2 = database.newVertex("V");
    OEdge e = database.newEdge(ver, ver1, "E");
    OEdge e1 = database.newEdge(ver, ver2, "E");
    database.save(e);
    database.save(e1);
    database.commit();
    database.close();
    orientDB.close();

    setup.setup();

    OServer server0Instance = setup.getServer(server0).getServerInstance();
    OServer server1Instance = setup.getServer(server1).getServerInstance();
    server1Instance
        .getDistributedManager()
        .waitUntilNodeOnline(server0Instance.getDistributedManager().getLocalNodeName(), "test");
    server0Instance
        .getDistributedManager()
        .waitUntilNodeOnline(server1Instance.getDistributedManager().getLocalNodeName(), "test");

    OrientDB embOri = server0Instance.getContext();
    ODatabaseSession data = embOri.open("test", "admin", "admin");
    try (OResultSet query = data.query("select from V")) {
      for (OResult res : query.stream().collect(Collectors.toList())) {
        OElement ele = res.getElement().get();
        ele.setProperty("name", "val");
        data.save(ele);
      }
    }
    data.close();
    embOri.drop("test");
    embOri.close();
  }

  @After
  public void after() {
    setup.teardown();
  }
}
