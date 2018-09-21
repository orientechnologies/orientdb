package com.orientechnologies.enterprise.server;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.operation.NodesManager;
import com.orientechnologies.enterprise.server.listener.OEnterpriseConnectionListener;
import com.orientechnologies.enterprise.server.listener.OEnterpriseStorageListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.*;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseLocalPaginatedStorage;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OSystemDatabase;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginInfo;
import com.orientechnologies.orient.server.security.OServerSecurity;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 16/07/2018.
 */
public class OEnterpriseServerImpl implements OEnterpriseServer, OServerPlugin, ODatabaseLifecycleListener {

  private final OEnterpriseAgent agent;
  private       OServer          server;

  private List<OEnterpriseConnectionListener> listeners = new ArrayList<>();

  private List<OEnterpriseStorageListener> dbListeners = new ArrayList<>();

  private Map<String, OEnterpriseLocalPaginatedStorage> storages = new ConcurrentHashMap<>();

  public OEnterpriseServerImpl(OServer server, OEnterpriseAgent agent) {
    this.server = server;
    this.agent = agent;
    server.getPluginManager().registerPlugin(new OServerPluginInfo("Enterprise Server", null, null, null, this, null, 0, null));
    Orient.instance().addDbLifecycleListener(this);
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
  public void startup() {

  }

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
  public OSystemDatabase getSystemDatabase() {
    return server.getSystemDatabase();
  }

  public NodesManager getNodesManager() {
    return agent.getNodesManager();
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
  public void config(OServer oServer, OServerParameterConfiguration[] oServerParameterConfigurations) {
  }

  @Override
  public void sendShutdown() {

  }

  @Override
  public Object getContent(String s) {
    return null;
  }

  @Override
  public void onCreate(ODatabaseInternal iDatabase) {
    OStorage storage = iDatabase.getStorage();
    if (storages.get(storage.getName()) == null) {
      if (storage instanceof OEnterpriseLocalPaginatedStorage) {
        OEnterpriseLocalPaginatedStorage s = (OEnterpriseLocalPaginatedStorage) storage;
        storages.put(storage.getName(), s);
        dbListeners.forEach((l) -> l.onOpen(s));
      }
    }
  }

  @Override
  public void onOpen(ODatabaseInternal iDatabase) {
    OStorage storage = iDatabase.getStorage();
    if (storage instanceof OEnterpriseLocalPaginatedStorage) {
      OEnterpriseLocalPaginatedStorage s = (OEnterpriseLocalPaginatedStorage) storage;
      if (storages.putIfAbsent(storage.getName(), s) == null) {
        storages.put(storage.getName(), s);
        dbListeners.forEach((l) -> l.onOpen(s));
      }
    }

  }

  @Override
  public void onClose(ODatabaseInternal iDatabase) {

  }

  @Override
  public void onDrop(ODatabaseInternal iDatabase) {

    OStorage storage = iDatabase.getStorage();
    if (storage instanceof OEnterpriseLocalPaginatedStorage) {
      if (storages.remove(storage.getName()) != null) {
        OEnterpriseLocalPaginatedStorage s = (OEnterpriseLocalPaginatedStorage) storage;
        dbListeners.forEach((l) -> l.onDrop(s));
      }
    }

  }

  @Override
  public void onLocalNodeConfigurationRequest(ODocument iConfiguration) {

  }

  @Override
  public void registerFunction(OSQLFunction function) {
    OSQLEngine.getInstance().registerFunction(function.getName(), function);

  }

  @Override
  public void registerStatelessCommand(OServerCommand iCommand) {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener != null) {
      listener.registerStatelessCommand(iCommand);
    }
  }

  @Override
  public void unregisterStatelessCommand(Class<? extends OServerCommand> iCommandClass) {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener != null) {
      listener.unregisterStatelessCommand(iCommandClass);
    }
  }

  @Override
  public Collection<OServerCommand> listCommands() {
    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
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
  public OServerSecurity getSecurity() {
    return server.getSecurity();
  }

  @Override
  public List<OResult> listQueries(Optional<Function<OClientConnection, Boolean>> filter) {
    return getConnections().stream().filter((c) -> c.getDatabase() != null && filter.map(f -> f.apply(c)).orElse(true))
        .flatMap((c) -> c.getDatabase().getActiveQueries().entrySet().stream().map((k) -> {
          OResultInternal internal = new OResultInternal();
          internal.setProperty("queryId", k.getKey());
          OResultSet resultSet = k.getValue();
          Optional<OExecutionPlan> plan = resultSet.getExecutionPlan();
          String query = plan.map((p -> {
            String q = "";
            if (p instanceof OInternalExecutionPlan) {
              String stm = ((OInternalExecutionPlan) p).getStatement();
              if (stm != null) {
                q = stm;
              }
            }
            return q;
          })).orElse("");

          String user = "-";

          if (c.getDatabase() != null && c.getDatabase().getUser() != null) {
            user = c.getDatabase().getUser().getName();
          }
          internal.setProperty("user", user);
          internal.setProperty("database", c.getDatabase().getName());
          internal.setProperty("query", query);
          if (resultSet instanceof OLocalResultSetLifecycleDecorator) {
            OResultSet oResultSet = ((OLocalResultSetLifecycleDecorator) resultSet).getInternal();
            if (oResultSet instanceof OLocalResultSet) {
              internal.setProperty("startTime", ((OLocalResultSet) oResultSet).getStartTime());
              internal.setProperty("elapsedTimeMillis", ((OLocalResultSet) oResultSet).getTotalExecutionTime());
            }
          }
          return internal;
        })).collect(Collectors.toList());
  }
}
