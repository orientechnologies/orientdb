package com.orientechnologies.enterprise.server;

import com.orientechnologies.agent.operation.NodesManager;
import com.orientechnologies.agent.services.OEnterpriseService;
import com.orientechnologies.agent.services.metrics.server.database.QueryInfo;
import com.orientechnologies.enterprise.server.listener.OEnterpriseConnectionListener;
import com.orientechnologies.enterprise.server.listener.OEnterpriseStorageListener;
import com.orientechnologies.orient.core.db.OSystemDatabase;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import java.util.*;
import java.util.function.Function;

/** Created by Enrico Risa on 16/07/2018. */
public interface OEnterpriseServer {

  void registerConnectionListener(OEnterpriseConnectionListener listener);

  void unregisterConnectionListener(OEnterpriseConnectionListener listener);

  void registerDatabaseListener(OEnterpriseStorageListener listener);

  void unRegisterDatabaseListener(OEnterpriseStorageListener listener);

  void registerFunction(OSQLFunction function);

  void registerStatelessCommand(final OServerCommand iCommand);

  void unregisterStatelessCommand(final Class<? extends OServerCommand> iCommandClass);

  Collection<OServerCommand> listCommands();

  void unregisterFunction(String function);

  default Map<String, String> getAvailableStorageNames() {
    return Collections.emptyMap();
  }

  void shutdown();

  List<OClientConnection> getConnections();

  OrientDBInternal getDatabases();

  void interruptConnection(Integer connectionId);

  OSystemDatabase getSystemDatabase();

  NodesManager getNodesManager();

  ODistributedServerManager getDistributedManager();

  boolean existsDatabase(String databaseName);

  void restore(String databaseName, String path);

  OSecuritySystem getSecurity();

  List<OResult> listQueries(Optional<Function<OClientConnection, Boolean>> filter);

  List<OResult> getQueryStats(Optional<String> database);

  Optional<QueryInfo> getQueryInfo(OResultSet resultSet);

  <T extends OEnterpriseService> Optional<T> getServiceByClass(Class<T> klass);
}
