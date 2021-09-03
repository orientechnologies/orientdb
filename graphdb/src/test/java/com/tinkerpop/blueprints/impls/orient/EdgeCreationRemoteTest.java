package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.server.OServer;
import com.tinkerpop.blueprints.Vertex;
import java.io.IOException;
import java.util.UUID;
import org.junit.*;

public class EdgeCreationRemoteTest {
  private OServer server;
  private final String dbName = "MAPP";
  private ORID app1Id;

  @Before
  public void setup()
      throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
    server = new OServer(false);
    server.startup(
        OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config-single-run.xml"));
    server.activate();
    connect("remote:localhost:3064", dbName, "root", "root", true);
    addPrerequisites();
  }

  @After
  public void tearDown() {
    disconnect(dbName, true);
    server.shutdown();
  }

  @AfterClass
  public static void afterClass() {
    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  private OrientDB orient;
  private ODatabasePool pool;

  public void disconnect(final String dbName, boolean cleanup) {
    pool.close();
    if (cleanup) {
      orient.drop(dbName);
    }
    orient.close();
  }

  @Test
  public void createEdgeFromANewVertexToAnExistingOne() {
    final OrientGraph g = new OrientGraph("remote:localhost:3064/MAPP");

    // timeline issue: APP1 and aAPP2 will fail. APP3 will be successfully committed.
    final String targetAppId = "APP1";
    final Vertex target = g.getVertices("LID", targetAppId).iterator().next();
    System.out.println(target.getProperty("ID").toString());

    final OrientVertex v1 = g.addVertex("class:KEYDOK");
    v1.setProperty("ID", UUID.randomUUID().toString());
    v1.addEdge("HAS_AS_FAVORITE", target);

    g.commit();
    g.shutdown();
  }

  @Test
  public void createEdgeFromANewVertexToAnExistingOneWithFactory() {
    final OrientGraphFactory factory =
        new OrientGraphFactory("remote:localhost:3064/MAPP", "admin", "admin").setupPool(5, 10);
    final OrientGraph g = factory.getTx();

    // APP1 and aAPP2 will fail. APP3 will be successfully committed.
    final String targetAppId = "APP1";
    final Vertex target = g.getVertices("LID", targetAppId).iterator().next();
    System.out.println(target.getProperty("ID").toString());

    final OrientVertex v1 = g.addVertex("class:KEYDOK");
    v1.setProperty("ID", UUID.randomUUID().toString());

    v1.addEdge("HAS_AS_FAVORITE", target);

    g.commit();
    g.shutdown();
    factory.close();
  }

  private void connect(
      final String serverName,
      final String dbName,
      final String userName,
      final String password,
      final boolean createDB) {
    final OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5);
    poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10);
    final OrientDBConfig oriendDBconfig = poolCfg.build();
    if (serverName.startsWith("remote:")) {
      // remote:<host> can be called like that
      orient = new OrientDB(serverName, userName, password, oriendDBconfig);
    } else if (serverName.startsWith("embedded:")) {
      // embedded:/<path>/directory + server can be called like that
      orient = new OrientDB(serverName, OrientDBConfig.defaultConfig());
    } else {
      throw new UnsupportedOperationException(
          "Currently only 'embedded' and 'remote' are supported.");
    }
    if (createDB) {
      orient.execute(
          "create database ? plocal users (admin identified by 'admin' role admin)", dbName);
    }
    pool = new ODatabasePool(orient, dbName, "admin", "admin", oriendDBconfig);
  }

  private void addPrerequisites() {
    try (final ODatabaseSession session = pool.acquire()) {
      final OClass appClass = session.createVertexClass("APP");
      appClass.createProperty("ID", OType.STRING);
      appClass.createProperty("LID", OType.STRING);

      final OClass keyDocClass = session.createVertexClass("KEYDOK");
      keyDocClass.createProperty("ID", OType.STRING);
      keyDocClass.createProperty("LID", OType.STRING);
      keyDocClass.createProperty("current", OType.BOOLEAN);
      keyDocClass.createProperty("insertedOn", OType.DATETIME);
      keyDocClass.createProperty("version", OType.INTEGER);

      final OClass underClass = session.createEdgeClass("UNDER");
      underClass.createProperty("insertedOn", OType.DATETIME);
      underClass.createProperty("isActive", OType.BOOLEAN);
      underClass.createProperty("versioning", OType.INTEGER);
      underClass.createProperty("since", OType.DATETIME);

      final OClass hasAsFavoriteClass = session.createEdgeClass("HAS_AS_FAVORITE");
      hasAsFavoriteClass.createProperty("insertedOn", OType.DATETIME);
      hasAsFavoriteClass.createProperty("isActive", OType.BOOLEAN);
      hasAsFavoriteClass.createProperty("isCurrent", OType.BOOLEAN);
      hasAsFavoriteClass.createProperty("versioning", OType.INTEGER);
      hasAsFavoriteClass.createProperty("since", OType.DATETIME);

      final OVertex app1 = session.newVertex(appClass);
      app1.setProperty("ID", "APP1-ID");
      app1.setProperty("LID", "APP1");
      app1.save();
      app1Id = app1.getIdentity();

      final OVertex app2 = session.newVertex(appClass);
      app2.setProperty("ID", "APP2-ID");
      app2.setProperty("LID", "APP2");
      app2.save();

      final OVertex app3 = session.newVertex(appClass);
      app3.setProperty("ID", "APP3-ID");
      app3.setProperty("LID", "APP3");
      app3.save();

      final OEdge edge = session.newEdge(app1, app2, underClass);
      edge.setProperty("isCurrent", true);
      edge.setProperty("isActive", true);
      edge.setProperty("versioning", 1);
      edge.setProperty("insertedOn", System.currentTimeMillis());
      edge.setProperty("since", System.currentTimeMillis());
      edge.save();
    }
  }
}
