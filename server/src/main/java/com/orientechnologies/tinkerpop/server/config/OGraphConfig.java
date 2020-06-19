package com.orientechnologies.tinkerpop.server.config;

import static org.apache.tinkerpop.gremlin.orientdb.OrientGraph.CONFIG_TRANSACTIONAL;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.tinkerpop.server.OrientGremlinGraphManager;
import com.orientechnologies.tinkerpop.server.auth.OGremlinServerAuthenticator;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import javax.script.Bindings;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphBaseFactory;
import org.apache.tinkerpop.gremlin.orientdb.OrientStandardGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings;

/** Created by Enrico Risa on 06/09/2017. */
public class OGraphConfig {

  private ODocument config;

  public OGraphConfig(ODocument config) {
    this.config = config;
  }

  public static OGraphConfig read(String file) throws Exception {
    final InputStream input = new FileInputStream(new File(file));
    try {
      return read(input);
    } finally {
      input.close();
    }
  }

  public static OGraphConfig read(InputStream input) throws Exception {

    Objects.requireNonNull(input);
    ODocument document = new ODocument().fromJSON(input);
    return new OGraphConfig(document);
  }

  public Map<String, Map<String, String>> getGraphs() {
    return config.field("graphs");
  }

  public void apply(
      OServer server,
      OrientGremlinGraphManager graphManager,
      Settings settings,
      Bindings bindings) {

    boolean hasServerAuthenticator = hasServerAuthenticator(settings);
    OrientDB context = server.getContext();
    getGraphs()
        .entrySet()
        .forEach(
            (k) -> {
              Map<String, String> val = k.getValue();
              String graphName = val.get("graph");
              String traversalName = val.get("traversal");
              String username = val.get("username");
              String password = val.get("password");

              if (graphName != null && traversalName != null) {

                if (hasServerAuthenticator || (username != null && password != null)) {
                  OrientGraphBaseFactory factory =
                      new OrientGraphBaseFactory() {
                        @Override
                        public OrientGraph getNoTx() {
                          BaseConfiguration configuration = new BaseConfiguration();
                          configuration.addProperty(CONFIG_TRANSACTIONAL, false);
                          return newGraph(
                              this, context, configuration, k.getKey(), username, password);
                        }

                        @Override
                        public OrientGraph getTx() {
                          BaseConfiguration configuration = new BaseConfiguration();
                          configuration.addProperty(CONFIG_TRANSACTIONAL, true);
                          return newGraph(
                              this, context, configuration, k.getKey(), username, password);
                        }
                      };
                  OrientStandardGraph graph =
                      new OrientStandardGraph(factory, new BaseConfiguration());
                  graphManager.putGraph(graphName, graph);
                  bindings.put(graphName, graph);
                  GraphTraversalSource traversal = graph.traversal();
                  graphManager.putTraversalSource(traversalName, traversal);
                  bindings.put(traversalName, traversal);
                } else {
                  OLogManager.instance()
                      .warn(this, "Cannot configure the graph %s since it's not protected", k);
                }
              } else {
                OLogManager.instance()
                    .warn(this, "Cannot configure the graph %s invalid graph/traversal alias", k);
              }
            });
  }

  private OrientGraph newGraph(
      OrientGraphBaseFactory factory,
      OrientDB context,
      BaseConfiguration configuration,
      String dbName,
      String username,
      String password) {
    ODatabaseDocument db;
    if (username != null && password != null) {
      db = context.open(dbName, username, password);
    } else {
      db = OrientDBInternal.extract(context).openNoAuthenticate(dbName, username);
    }
    return new OrientGraph(factory, db, configuration, username, password);
  }

  private boolean hasServerAuthenticator(Settings settings) {

    if (settings.authentication != null && settings.authentication.authenticator != null) {
      return settings.authentication.authenticator.equals(
          OGremlinServerAuthenticator.class.getName());
    }
    return false;
  }

  public void reload(ODocument config) {}

  public ODocument getConfig() {
    return config;
  }
}
