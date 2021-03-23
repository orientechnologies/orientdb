package com.orientechnologies.tinkerpop;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import com.orientechnologies.tinkerpop.server.OGremlinServerPlugin;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/** Created by Enrico Risa on 14/03/17. */
public class AbstractRemoteTest {

  protected static final String SERVER_DIRECTORY = "./target";

  protected OServer server;

  @Rule public TestName name = new TestName();

  public InputStream getInputConfig() {
    return ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
  }

  @Before
  public void setup() throws Exception {

    System.setProperty("ORIENTDB_HOME", SERVER_DIRECTORY);

    InputStream stream = getInputConfig();

    server = OServerMain.create(false);
    server.startup(stream);
    server.activate();

    server
        .getDatabases()
        .executeServerStatement(
            "create database "
                + name.getMethodName()
                + " memory users (admin identified by 'admin' role admin,"
                + "reader identified by 'reader' role reader,"
                + "writer identified by 'writer' role writer )",
            "root",
            "root");
  }

  protected void installGremlinServer() {
    OGremlinServerPlugin gremlinServer =
        new OGremlinServerPlugin() {

          @Override
          public InputStream getServerConfig() throws FileNotFoundException {
            return ClassLoader.getSystemResourceAsStream("gremlin-server.yaml");
          }

          @Override
          public InputStream getGraphsConfig() throws FileNotFoundException {
            ODocument doc = createGraphConfig();
            return new ByteArrayInputStream(doc.toJSON().getBytes());
          }
        };

    server
        .getPluginManager()
        .registerPlugin(
            new OServerPluginInfo(
                gremlinServer.getName(), null, null, null, gremlinServer, null, 0, null));
    gremlinServer.config(server, null);

    gremlinServer.onAfterActivate();

    BaseConfiguration configuration = new BaseConfiguration();
    configuration.addProperty(OrientGraph.CONFIG_DB_NAME, name.getMethodName());
    onConfiguration(configuration);
    gremlinServer.installCustomGraph(configuration, "graph", "g");
  }

  protected void onConfiguration(BaseConfiguration configuration) {}

  protected ODocument createGraphConfig() {
    ODocument doc = new ODocument();
    Map<String, Map<String, String>> graphs = new HashMap<>();
    Map<String, String> currentGraph = new HashMap<>();
    currentGraph.put("graph", "graph");
    currentGraph.put("traversal", "g");
    graphs.put(name.getMethodName(), currentGraph);
    doc.field("graphs", graphs);
    return doc;
  }

  protected ODatabaseDocument openLocalDB() {
    return server.openDatabase(name.getMethodName(), "admin", "admin");
  }

  @After
  public void teardown() {

    server.shutdown();
  }
}
