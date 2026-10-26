package com.orientechnologies.spatial;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Spatial index changes made in a remote transaction travel over the binary protocol. */
public class LuceneSpatialRemoteTxTest {

  private static final String DB_NAME = "LuceneSpatialRemoteTxTest";

  private OServer server;
  private OrientDB remote;

  @Before
  public void before() throws Exception {
    server =
        OServer.startFromClasspathConfig(
            "com/orientechnologies/spatial/spatial-remote-tx-server-config.xml");
    int port =
        server.getListenerByProtocol(ONetworkProtocolBinary.class).getInboundAddr().getPort();
    remote =
        new OrientDB("remote:localhost:" + port, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database " + DB_NAME + " plocal users(admin identified by 'admin' role admin)");
    try (ODatabaseSession session = remote.open(DB_NAME, "admin", "admin")) {
      session.command("create class City extends V").close();
      session.command("create property City.location EMBEDDED OPoint").close();
      session.command("create property City.name STRING").close();
      session.command("create index City.location on City(location) SPATIAL ENGINE LUCENE").close();
    }
  }

  @Test
  public void testSpatialIndexChangesInRemoteTx() {
    try (ODatabaseSession session = remote.open(DB_NAME, "admin", "admin")) {
      session.begin();
      // Each command runs on the server inside the transaction, so the spatial index changes are
      // sent to the client and back to the server with the next command and the commit.
      insertCity(session, "Rome", 12.5, 41.9);
      insertCity(session, "London", -0.12, 51.5);
      session.commit();

      Assert.assertEquals(1, countWithin(session, 12.31, 41.82, 12.66, 41.96));
      Assert.assertEquals(1, countWithin(session, -0.5, 51.2, 0.3, 51.7));
      Assert.assertEquals(2, countWithin(session, -1.0, 41.0, 13.0, 52.0));
    }
  }

  private static void insertCity(
      ODatabaseSession session, String name, double longitude, double latitude) {
    session
        .command(
            "insert into City set name = ?, location = {'@class': 'OPoint', 'coordinates': ["
                + longitude
                + ", "
                + latitude
                + "]}",
            name)
        .close();
  }

  private static long countWithin(
      ODatabaseSession session, double minX, double minY, double maxX, double maxY) {
    String query =
        "select from City where ST_WITHIN(location, { 'shape' : { 'type' : 'ORectangle',"
            + " 'coordinates' : ["
            + minX
            + ", "
            + minY
            + ", "
            + maxX
            + ", "
            + maxY
            + "]} }) = true";
    try (OResultSet rs = session.query(query)) {
      return rs.stream().count();
    }
  }

  @After
  public void after() {
    remote.drop(DB_NAME);
    remote.close();
    server.shutdown();
  }
}
