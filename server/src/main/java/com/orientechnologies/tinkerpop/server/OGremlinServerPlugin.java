package com.orientechnologies.tinkerpop.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginConfigurable;
import com.orientechnologies.tinkerpop.server.config.OGraphConfig;
import java.io.*;
import java.util.concurrent.CompletableFuture;
import javax.script.Bindings;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientEmbeddedFactory;
import org.apache.tinkerpop.gremlin.orientdb.OrientStandardGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;

/** Created by Enrico Risa on 06/09/2017. */
public class OGremlinServerPlugin extends OServerPluginAbstract
    implements OServerPluginConfigurable, OServerLifecycleListener {

  protected GremlinServer gremlinServer;
  protected OServer oServer;
  protected OrientGremlinGraphManager graphManager;
  protected ServerGremlinExecutor executor;
  protected OGraphConfig config;

  @Override
  public String getName() {
    return "gremlin-server";
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {

    this.oServer = oServer;
    this.oServer.registerLifecycleListener(this);
    this.config = new OGraphConfig(new ODocument());
  }

  public InputStream getServerConfig() throws FileNotFoundException {
    String configFile =
        OSystemVariableResolver.resolveSystemVariables(
            "${ORIENTDB_HOME}/config/gremlin-server.yaml");
    return new FileInputStream(new File(configFile));
  }

  public InputStream getGraphsConfig() throws FileNotFoundException {
    String aliasConfig =
        OSystemVariableResolver.resolveSystemVariables("${ORIENTDB_HOME}/config/graph-config.json");
    return new FileInputStream(new File(aliasConfig));
  }

  @Override
  public ODocument getConfig() {
    return config.getConfig();
  }

  @Override
  public void changeConfig(ODocument document) {
    config.reload(document);
  }

  @Override
  public void onBeforeActivate() {}

  @Override
  public void onAfterActivate() {

    Settings settings;
    InputStream cfg = null;
    try {
      OLogManager.instance().info(this, "Gremlin Server is starting up...");
      cfg = getServerConfig();
      settings = Settings.read(cfg);
      gremlinServer = new GremlinServer(settings);
      CompletableFuture<ServerGremlinExecutor> start = gremlinServer.start();

      this.executor = start.join();
      this.graphManager = (OrientGremlinGraphManager) executor.getGraphManager();

      OLogManager.instance().info(this, "Gremlin started correctly");
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on Gremlin Server startup", e);
    } finally {
      if (cfg != null) {
        try {
          cfg.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public void onBeforeDeactivate() {

    OLogManager.instance().info(this, "Gremlin Server is shutting down.");
    gremlinServer.stop().join();
    OLogManager.instance().info(this, "Gremlin Server shutting down completed.");
  }

  @Override
  public void onAfterDeactivate() {}

  public void installCustomGraph(
      BaseConfiguration configuration, String graphName, String traversalName) {
    OrientStandardGraph graph = OrientEmbeddedFactory.open(configuration);
    Bindings bindings = executor.getGremlinExecutor().getScriptEngineManager().getBindings();
    GraphTraversalSource traversal = graph.traversal();

    graphManager.putGraph(graphName, graph);
    bindings.put(graphName, graph);
    graphManager.putTraversalSource(traversalName, traversal);
    bindings.put(traversalName, traversal);
  }
}
