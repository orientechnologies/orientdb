package com.orientechnologies.enterprise.server;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.operation.NodesManager;
import com.orientechnologies.agent.profiler.metrics.OHistogram;
import com.orientechnologies.agent.profiler.metrics.OSnapshot;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsService;
import com.orientechnologies.agent.services.metrics.server.database.QueryInfo;
import com.orientechnologies.enterprise.server.listener.OEnterpriseConnectionListener;
import com.orientechnologies.enterprise.server.listener.OEnterpriseStorageListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.OQueryDatabaseState;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OQueryMetrics;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseLocalPaginatedStorage;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Created by Enrico Risa on 16/07/2018. */
public class OEnterpriseServerImpl
    implements OEnterpriseServer,
        OServerPlugin,
        ODatabaseLifecycleListener,
        ODatabaseListener,
        OServerLifecycleListener {
  private final OEnterpriseAgent agent;
  private OServer server;

  private NodesManager nodesManager;

  private List<OEnterpriseConnectionListener> listeners = new ArrayList<>();

  private List<OEnterpriseStorageListener> dbListeners = new ArrayList<>();

  private Map<String, OEnterpriseLocalPaginatedStorage> storages = new ConcurrentHashMap<>();

  public OEnterpriseServerImpl(final OServer server, final OEnterpriseAgent agent) {
    this.server = server;
    this.agent = agent;
    server
        .getPluginManager()
        .registerPlugin(
            new OServerPluginInfo("Enterprise Server", null, null, null, this, null, 0, null));
    Orient.instance().addDbLifecycleListener(this);

    this.server.registerLifecycleListener(this);
  }

  @Override
  public void registerConnectionListener(OEnterpriseConnectionListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unregisterConnectionListener(OEnterpriseConnectionListener listener) {
    listeners.remove(listener);
  }

  @Override
  public void registerDatabaseListener(OEnterpriseStorageListener listener) {
    dbListeners.add(listener);
  }

  @Override
  public void unRegisterDatabaseListener(OEnterpriseStorageListener listener) {
    dbListeners.remove(listener);
  }

  @Override
  public Map<String, String> getAvailableStorageNames() {
    return server.getAvailableStorageNames();
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void startup() {}

  @Override
  public void shutdown() {
    listeners.clear();
  }

  @Override
  public OrientDBInternal getDatabases() {
    return server.getDatabases();
  }

  @Override
  public void interruptConnection(Integer connectionId) {
    server.getClientConnectionManager().kill(connectionId);
  }

  @Override
  public void onClientConnection(OClientConnection oClientConnection) {

    this.listeners.forEach((l) -> l.onClientConnection(oClientConnection));
  }

  @Override
  public void onSocketAccepted(ONetworkProtocol protocol) {
    this.listeners.forEach((l) -> l.onSocketAccepted(protocol));
  }

  @Override
  public void onSocketDestroyed(ONetworkProtocol protocol) {
    this.listeners.forEach((l) -> l.onSocketDestroyed(protocol));
  }

  @Override
  public OSystemDatabase getSystemDatabase() {
    return server.getSystemDatabase();
  }

  public NodesManager getNodesManager() {
    return nodesManager;
  }

  @Override
  public ODistributedServerManager getDistributedManager() {
    return server.getDistributedManager();
  }

  @Override
  public boolean existsDatabase(String databaseName) {
    return server.existsDatabase(databaseName);
  }

  @Override
  public void restore(String databaseName, String path) {
    server.restore(databaseName, path);
  }

  @Override
  public void onClientDisconnection(OClientConnection oClientConnection) {
    this.listeners.forEach((l) -> l.onClientDisconnection(oClientConnection));
  }

  @Override
  public void onBeforeClientRequest(OClientConnection oClientConnection, byte b) {
    this.listeners.forEach((l) -> l.onBeforeClientRequest(oClientConnection, b));
  }

  @Override
  public void onAfterClientRequest(OClientConnection oClientConnection, byte b) {
    this.listeners.forEach((l) -> l.onAfterClientRequest(oClientConnection, b));
  }

  @Override
  public void onClientError(OClientConnection oClientConnection, Throwable throwable) {
    this.listeners.forEach((l) -> l.onClientError(oClientConnection, throwable));
  }

  @Override
  public void config(
      OServer oServer, OServerParameterConfiguration[] oServerParameterConfigurations) {}

  @Override
  public void sendShutdown() {}

  @Override
  public Object getContent(String s) {
    return null;
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @Override
  public void onAfterActivate() {

    var databases = server.getDatabases();
    if (databases instanceof OrientDBDistributed) {
      nodesManager = new NodesManager((OrientDBDistributed) databases);
    }
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    if (!((ODatabaseDocumentInternal) iDatabase).isRemote()) {
      OStorage storage = iDatabase.getStorage();
      if (storages.get(storage.getName()) == null) {
        if (storage instanceof OEnterpriseLocalPaginatedStorage) {
          OEnterpriseLocalPaginatedStorage s = (OEnterpriseLocalPaginatedStorage) storage;
          storages.put(storage.getName(), s);
          dbListeners.forEach((l) -> l.onOpen(s));
        }
      }
    }
    iDatabase.registerListener(this);
  }

  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    if (!((ODatabaseDocumentInternal) iDatabase).isRemote()) {
      final OStorage storage = iDatabase.getStorage();
      if (storage instanceof OEnterpriseLocalPaginatedStorage) {
        OEnterpriseLocalPaginatedStorage s = (OEnterpriseLocalPaginatedStorage) storage;
        if (storages.putIfAbsent(storage.getName(), s) == null) {
          storages.put(storage.getName(), s);
          dbListeners.forEach((l) -> l.onOpen(s));
        }
      }
    }
    iDatabase.registerListener(this);
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    if (!((ODatabaseDocumentInternal) iDatabase).isRemote()) {
      final OStorage storage = iDatabase.getStorage();
      if (storage instanceof OEnterpriseLocalPaginatedStorage) {
        if (storages.remove(storage.getName()) != null) {
          OEnterpriseLocalPaginatedStorage s = (OEnterpriseLocalPaginatedStorage) storage;
          dbListeners.forEach((l) -> l.onDrop(s));
        }
      }
    }
  }

  @Override
  public void registerFunction(OSQLFunction function) {
    OSQLEngine.getInstance().registerFunction(function.getName(), function);
  }

  @Override
  public void registerStatelessCommand(OServerCommand iCommand) {
    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener != null) {
      listener.registerStatelessCommand(iCommand);
    }
  }

  @Override
  public void unregisterStatelessCommand(Class<? extends OServerCommand> iCommandClass) {
    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener != null) {
      listener.unregisterStatelessCommand(iCommandClass);
    }
  }

  @Override
  public Collection<OServerCommand> listCommands() {
    final OServerNetworkListener listener =
        server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    return listener.getStatelessCommands();
  }

  @Override
  public List<OClientConnection> getConnections() {
    return server.getClientConnectionManager().getConnections();
  }

  @Override
  public void unregisterFunction(String function) {
    OSQLEngine.getInstance().unregisterFunction(function);
  }

  @Override
  public OSecuritySystem getSecurity() {
    return server.getSecurity();
  }

  @Override
  public List<OResult> listQueries(Optional<Function<OClientConnection, Boolean>> filter) {
    return getConnections().stream()
        .filter((c) -> c.getDatabase() != null && filter.map(f -> f.apply(c)).orElse(true))
        .flatMap(
            (c) ->
                new HashMap<>(c.getDatabase().getActiveQueries())
                    .entrySet().stream()
                        .map(
                            (k) -> {
                              OResultInternal internal = new OResultInternal();
                              internal.setProperty("queryId", k.getKey());
                              OQueryDatabaseState resultSet = k.getValue();

                              String user = "-";

                              if (c.getDatabase() != null && c.getDatabase().getUser() != null) {
                                user = c.getDatabase().getUser().getName();
                              }
                              internal.setProperty("sessionId", c.getId());
                              internal.setProperty("user", user);
                              internal.setProperty("database", c.getDatabase().getName());

                              Optional<QueryInfo> info = getQueryInfo(resultSet.getResultSet());

                              info.ifPresent(
                                  (it) -> {
                                    internal.setProperty("language", it.getLanguage());
                                    internal.setProperty("query", it.getStatement());
                                    internal.setProperty("startTime", it.getStartTime());
                                    internal.setProperty(
                                        "elapsedTimeMillis", it.getElapsedTimeMillis());
                                  });

                              return internal;
                            }))
        .collect(Collectors.toList());
  }

  @Override
  public List<OResult> getQueryStats(Optional<String> database) {

    return agent
        .getServiceByClass(OrientDBMetricsService.class)
        .map(
            (service) -> {
              SortedMap<String, OHistogram> histograms =
                  service
                      .getRegistry()
                      .getHistograms((name, metric) -> name.matches("(?s)db.*.query.*"));
              return histograms.entrySet().stream()
                  .sorted(
                      (v1, v2) -> {
                        OSnapshot snapshot1 = v1.getValue().getSnapshot();
                        OSnapshot snapshot2 = v2.getValue().getSnapshot();
                        return Double.compare(snapshot2.getMean(), snapshot1.getMean());
                      })
                  .map(
                      (e) -> {
                        OResultInternal result = new OResultInternal();
                        String key = e.getKey();
                        OHistogram h = e.getValue();
                        OSnapshot snapshot = h.getSnapshot();
                        String statement = key.substring(key.indexOf(".query.") + 7);
                        String language = statement.substring(0, statement.indexOf("."));
                        String query = statement.substring(statement.indexOf(".") + 1);
                        String db = key.substring(key.indexOf("db.") + 3, key.indexOf(".query."));

                        result.setProperty("database", db);
                        result.setProperty("language", language);
                        result.setProperty("query", query);

                        result.setProperty("count", h.getCount());
                        result.setProperty("max", snapshot.getMax());
                        result.setProperty("min", snapshot.getMin());
                        result.setProperty("mean", snapshot.getMean());
                        return (OResult) result;
                      })
                  .filter(
                      f ->
                          database
                              .map(e -> e.equalsIgnoreCase(f.getProperty("database")))
                              .orElse(true))
                  .collect(Collectors.toList());
            })
        .orElse(new ArrayList());
  }

  @Override
  public void onCommandStart(ODatabase database, OResultSet result) {
    this.dbListeners.forEach((c -> c.onCommandStart(database, result)));
  }

  @Override
  public void onCommandEnd(ODatabase database, OResultSet result) {
    this.dbListeners.forEach((c -> c.onCommandEnd(database, result)));
  }

  @Override
  public Optional<QueryInfo> getQueryInfo(final OResultSet resultSet) {
    Optional<QueryInfo> info = Optional.empty();
    if (resultSet instanceof OLocalResultSetLifecycleDecorator) {
      OResultSet oResultSet = ((OLocalResultSetLifecycleDecorator) resultSet).getInternal();
      if (oResultSet instanceof OQueryMetrics) {
        OQueryMetrics oQueryMetrics = (OQueryMetrics) oResultSet;
        info =
            Optional.of(
                new QueryInfo(
                    oQueryMetrics.getStatement(),
                    oQueryMetrics.getLanguage(),
                    oQueryMetrics.getStartTime(),
                    oQueryMetrics.getElapsedTimeMillis()));
      }
    }
    return info;
  }

  @Override
  public <T extends OEnterpriseService> Optional<T> getServiceByClass(Class<T> klass) {
    return agent.getServiceByClass(klass);
  }
}
