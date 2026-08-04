/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.client.remote;

import static com.orientechnologies.orient.client.remote.ORemoteClient.ADDRESS_SEPARATOR;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_SOCKET_RETRY;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.ORemoteClient.CONNECTION_STRATEGY;
import com.orientechnologies.orient.client.remote.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.client.remote.db.document.OSharedContextRemote;
import com.orientechnologies.orient.client.remote.message.OConnect37Request;
import com.orientechnologies.orient.client.remote.message.OConnectResponse;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusResponse;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationResponse;
import com.orientechnologies.orient.client.remote.message.OListDatabasesRequest;
import com.orientechnologies.orient.client.remote.message.OListDatabasesResponse;
import com.orientechnologies.orient.client.remote.message.OListGlobalConfigurationsRequest;
import com.orientechnologies.orient.client.remote.message.OListGlobalConfigurationsResponse;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseResponse;
import com.orientechnologies.orient.client.remote.message.ORemoteResultSet;
import com.orientechnologies.orient.client.remote.message.OServerInfoRequest;
import com.orientechnologies.orient.client.remote.message.OServerInfoResponse;
import com.orientechnologies.orient.client.remote.message.OServerQueryRequest;
import com.orientechnologies.orient.client.remote.message.OServerQueryResponse;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationResponse;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OAdminSession;
import com.orientechnologies.orient.core.db.OCachedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.OCachedDatabasePoolFactoryImpl;
import com.orientechnologies.orient.core.db.OCancellableTimer;
import com.orientechnologies.orient.core.db.OCancellableTimerTask;
import com.orientechnologies.orient.core.db.OCreateDatabaseParameters;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePoolImpl;
import com.orientechnologies.orient.core.db.ODatabasePoolInternal;
import com.orientechnologies.orient.core.db.ODatabaseTask;
import com.orientechnologies.orient.core.db.ODatabaseTaskNoResult;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Created by tglman on 08/04/16. */
public class OrientDBRemote implements OrientDBInternal {
  private static final OLogger logger = OLogManager.instance().logger(OrientDBRemote.class);
  protected final Map<String, OSharedContextRemote> sharedContexts = new HashMap<>();
  private final Set<ODatabasePoolInternal> pools = new HashSet<>();
  private final String[] hosts;
  private final OrientDBConfig configurations;
  private final Orient orient;
  private final OCachedDatabasePoolFactory cachedPoolFactory;
  protected volatile ORemoteConnectionManager connectionManager;
  private volatile boolean open = true;
  private final Timer timer;
  private final ORemoteURLs urls;
  private final ExecutorService executor;

  public OrientDBRemote(String[] hosts, OrientDBConfig configurations, Orient orient) {
    super();

    this.hosts = hosts;
    this.orient = orient;
    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();
    timer = new Timer("Remote background operations timer", true);
    connectionManager =
        new ORemoteConnectionManager(this.configurations.getConfigurations(), timer);
    orient.addOrientDB(this);
    cachedPoolFactory = createCachedDatabasePoolFactory(this.configurations);
    urls = new ORemoteURLs(hosts, this.configurations.getConfigurations());
    int size =
        this.configurations
            .getConfigurations()
            .getValueAsInteger(OGlobalConfiguration.EXECUTOR_POOL_MAX_SIZE);
    if (size == -1) {
      size = Runtime.getRuntime().availableProcessors() / 2;
    }
    if (size <= 0) {
      size = 1;
    }

    executor =
        OThreadPoolExecutors.newScalingThreadPool(
            "OrientDBRemote", 0, size, 100, 1, TimeUnit.MINUTES);
  }

  protected OCachedDatabasePoolFactory createCachedDatabasePoolFactory(OrientDBConfig config) {
    int capacity =
        config.getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY);
    long timeout =
        config
            .getConfigurations()
            .getValueAsInteger(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT);
    return new OCachedDatabasePoolFactoryImpl(this, capacity, timeout);
  }

  private String buildUrl(String name) {
    return String.join(ADDRESS_SEPARATOR, hosts) + "/" + name;
  }

  public ODatabaseDocumentInternal open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  @Override
  public ODatabaseDocumentInternal open(
      String name, String user, String password, OrientDBConfig config) {
    checkOpen();
    OrientDBConfig resolvedConfig = solveConfig(config);
    try {
      ODatabaseDocumentRemote db =
          new ODatabaseDocumentRemote(
              getOrCreateSharedContext(name, resolvedConfig.getConfigurations()));
      db.internalOpen(user, password, resolvedConfig);
      return db;
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  public ODatabaseDocumentInternal open(
      OAuthenticationInfo authenticationInfo, OrientDBConfig config) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(String name, String user, String password, ODatabaseType databaseType) {
    create(name, user, password, databaseType, null);
  }

  @Override
  public synchronized void create(
      String name,
      String user,
      String password,
      ODatabaseType databaseType,
      OrientDBConfig config) {

    config = solveConfig(config);

    if (name == null || name.length() <= 0 || name.contains("`")) {
      final String message = "Cannot create unnamed remote storage. Check your syntax";
      logger.error(message, null);
      throw new OStorageException(message);
    }
    String create = String.format("CREATE DATABASE `%s` %s ", name, databaseType.name());
    Map<String, Object> parameters = new HashMap<String, Object>();
    Set<String> keys = config.getConfigurations().getContextKeys();
    if (!keys.isEmpty()) {
      List<String> entries = new ArrayList<String>();
      for (String key : keys) {
        OGlobalConfiguration globalKey = OGlobalConfiguration.findByKey(key);
        entries.add(String.format("\"%s\": :%s", key, globalKey.name()));
        parameters.put(globalKey.name(), config.getConfigurations().getValue(globalKey));
      }
      create += String.format("{\"config\":{%s}}", String.join(",", entries));
    }

    executeServerStatement(create, user, password, parameters);
  }

  public ODatabaseDocumentRemotePooled poolOpen(
      String name, String user, String password, ODatabasePoolInternal pool) {
    OrientDBConfig solved = solveConfig(pool.getConfig());
    ODatabaseDocumentRemotePooled db =
        new ODatabaseDocumentRemotePooled(
            pool, getOrCreateSharedContext(name, solved.getConfigurations()));
    db.internalOpen(user, password, pool.getConfig());
    return db;
  }

  public ODocument getServerInfo(String username, String password) {
    OServerInfoRequest request = new OServerInfoRequest();
    OServerInfoResponse response = connectAndSend(null, username, password, request);
    ODocument res = new ODocument();
    res.fromJSON(response.getResult());

    return res;
  }

  public ODocument getClusterStatus(String username, String password) {
    ODistributedStatusRequest request = new ODistributedStatusRequest("status");
    ODistributedStatusResponse response = connectAndSend(null, username, password, request);

    logger.debug("Cluster status %s", response.getClusterConfig().toJSON("prettyPrint"));
    return response.getClusterConfig();
  }

  public String getGlobalConfiguration(
      String username, String password, OGlobalConfiguration config) {
    OGetGlobalConfigurationRequest request = new OGetGlobalConfigurationRequest(config.getKey());
    OGetGlobalConfigurationResponse response = connectAndSend(null, username, password, request);
    return response.getValue();
  }

  public void setGlobalConfiguration(
      String username, String password, OGlobalConfiguration config, String iConfigValue) {
    String value = iConfigValue != null ? iConfigValue : "";
    OSetGlobalConfigurationRequest request =
        new OSetGlobalConfigurationRequest(config.getKey(), value);
    OSetGlobalConfigurationResponse response = connectAndSend(null, username, password, request);
  }

  public Map<String, String> getGlobalConfigurations(String username, String password) {
    OListGlobalConfigurationsRequest request = new OListGlobalConfigurationsRequest();
    OListGlobalConfigurationsResponse response = connectAndSend(null, username, password, request);
    return response.getConfigs();
  }

  public ORemoteConnectionManager getConnectionManager() {
    return connectionManager;
  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    OExistsDatabaseRequest request = new OExistsDatabaseRequest(name, null);
    OExistsDatabaseResponse response = connectAndSend(name, user, password, request);
    return response.isExists();
  }

  @Override
  public synchronized void drop(String name, String user, String password) {
    ODropDatabaseRequest request = new ODropDatabaseRequest(name, null);
    ODropDatabaseResponse response = connectAndSend(name, user, password, request);

    OSharedContext ctx = sharedContexts.get(name);
    if (ctx != null) {
      ctx.close();
      sharedContexts.remove(name);
    }
  }

  @Override
  public void internalDrop(String database) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> listDatabases(String user, String password) {
    return getDatabases(user, password).keySet();
  }

  public Map<String, String> getDatabases(String user, String password) {
    OListDatabasesRequest request = new OListDatabasesRequest();
    OListDatabasesResponse response = connectAndSend(null, user, password, request);
    return response.getDatabases();
  }

  @Override
  public void restore(
      String name,
      String user,
      String password,
      ODatabaseType type,
      String path,
      OrientDBConfig config) {
    if (name == null || name.length() <= 0) {
      final String message = "Cannot create unnamed remote storage. Check your syntax";
      logger.error(message, null);
      throw new OStorageException(message);
    }

    OCreateDatabaseRequest request =
        new OCreateDatabaseRequest(name, type.name().toLowerCase(), null, path);

    OCreateDatabaseResponse response = connectAndSend(name, user, password, request);
  }

  public <T extends OBinaryResponse> T connectAndSend(
      String name, String user, String password, OBinaryRequest<T> request) {
    return connectAndExecute(
        name,
        user,
        password,
        session -> {
          return networkAdminOperation(
              request, session, "Error sending request:" + request.getDescription());
        });
  }

  public ODatabasePoolInternal openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }

  @Override
  public ODatabasePoolInternal openPool(
      String name, String user, String password, OrientDBConfig config) {
    checkOpen();
    ODatabasePoolImpl pool = new ODatabasePoolImpl(this, name, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  @Override
  public ODatabasePoolInternal cachedPool(String database, String user, String password) {
    return cachedPool(database, user, password, null);
  }

  @Override
  public ODatabasePoolInternal cachedPool(
      String database, String user, String password, OrientDBConfig config) {
    checkOpen();
    ODatabasePoolInternal pool =
        cachedPoolFactory.get(database, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  public void removePool(ODatabasePoolInternal pool) {
    pools.remove(pool);
  }

  @Override
  public void close() {
    if (!open) return;
    removeShutdownHook();
    internalClose();
  }

  public void internalClose() {
    if (!open) return;

    if (timer != null) {
      timer.cancel();
    }

    final List<OSharedContextRemote> storagesCopy;
    synchronized (this) {
      // SHUTDOWN ENGINES AVOID OTHER OPENS
      open = false;
      storagesCopy = new ArrayList<>(this.sharedContexts.values());
    }

    for (OSharedContextRemote ctx : storagesCopy) {
      try {
        ctx.close();
      } catch (Exception e) {
        logger.warn("-- error on shutdown storage", e);
      } catch (Error e) {
        logger.warn("-- error on shutdown storage", e);
        throw e;
      }
    }
    synchronized (this) {
      this.sharedContexts.clear();
      connectionManager.close();
    }
  }

  private OrientDBConfig solveConfig(OrientDBConfig config) {
    if (config != null) {
      config.setParent(this.configurations);
      return config;
    } else {
      OrientDBConfig cfg = OrientDBConfig.defaultConfig();
      cfg.setParent(this.configurations);
      return cfg;
    }
  }

  private void checkOpen() {
    if (!open) throw new ODatabaseException("OrientDB Instance is closed");
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isEmbedded() {
    return false;
  }

  @Override
  public void removeShutdownHook() {
    orient.removeOrientDB(this);
  }

  @Override
  public void loadAllDatabases() {
    // In remote does nothing
  }

  @Override
  public ODatabaseDocumentInternal openNoAuthenticate(String iDbUrl, String user) {
    throw new UnsupportedOperationException(
        "Open with no authentication is not supported in remote");
  }

  @Override
  public void initCustomStorage(String name, String baseUrl, String userName, String userPassword) {
    throw new UnsupportedOperationException("Custom storage is not supported in remote");
  }

  @Override
  public Collection<OStorage> getStorages() {
    throw new UnsupportedOperationException("List storage is not supported in remote");
  }

  @Override
  public synchronized void forceDatabaseClose(String databaseName) {
    OSharedContextRemote ctx = sharedContexts.remove(databaseName);
    if (ctx != null) {
      ctx.close();
    }
  }

  @Override
  public void restore(
      String name,
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      OCommandOutputListener iListener) {
    throw new UnsupportedOperationException("raw restore is not supported in remote");
  }

  @Override
  public ODatabaseDocumentInternal openNoAuthorization(String name) {
    throw new UnsupportedOperationException(
        "impossible skip authentication and authorization in remote");
  }

  protected synchronized OSharedContextRemote getOrCreateSharedContext(
      String name, OContextConfiguration config) {
    OSharedContextRemote result = sharedContexts.get(name);
    if (result == null) {
      result = createSharedContext(name, config);
      sharedContexts.put(name, result);
    }
    return result;
  }

  private OSharedContextRemote createSharedContext(String name, OContextConfiguration config) {
    ORemoteClient storage = new ORemoteClient(urls, name, this, connectionManager, config);
    return new OSharedContextRemote(storage, this);
  }

  public void schedule(TimerTask task, long delay, long period) {
    timer.schedule(task, delay, period);
  }

  public void scheduleOnce(TimerTask task, long delay) {
    timer.schedule(task, delay);
  }

  @Override
  public OCancellableTimer delayExecute(Runnable toExecuted, long delay) {
    TimerTask tt =
        new TimerTask() {
          @Override
          public void run() {
            execute(toExecuted);
          }
        };
    timer.schedule(tt, delay);
    return new OCancellableTimerTask(tt);
  }

  @Override
  public OCancellableTimer periodicExecute(Runnable toExecuted, long periodic) {
    TimerTask tt =
        new TimerTask() {
          @Override
          public void run() {
            execute(toExecuted);
          }
        };
    timer.schedule(tt, periodic, periodic);
    return new OCancellableTimerTask(tt);
  }

  @Override
  public OCancellableTimerTask scheduleExecuteFrom(
      Runnable toExecuted, Date firstTime, long period) {
    long first = Math.max(0, firstTime.getTime() - System.currentTimeMillis());
    TimerTask tt =
        new TimerTask() {
          @Override
          public void run() {
            execute(toExecuted);
          }
        };
    timer.schedule(tt, first, period);
    return new OCancellableTimerTask(tt);
  }

  @Override
  public <X> Future<X> executeNoAuthorization(String database, ODatabaseTask<X> task) {
    throw new UnsupportedOperationException("execute with no session not available in remote");
  }

  @Override
  public void executeNoAuthorizationOnActive(String database, ODatabaseTaskNoResult task) {
    throw new UnsupportedOperationException("execute with no session not available in remote");
  }

  @Override
  public <X> Future<X> execute(String database, String user, ODatabaseTask<X> task) {
    throw new UnsupportedOperationException("execute with no session not available in remote");
  }

  @Override
  public Future<?> execute(Runnable task) {
    return executor.submit(task);
  }

  @Override
  public <X> Future<X> execute(Callable<X> task) {
    return executor.submit(task);
  }

  public void releaseDatabase(String database, String user, String password) {
    OReleaseDatabaseRequest request = new OReleaseDatabaseRequest(database, null);
    OReleaseDatabaseResponse response = connectAndSend(database, user, password, request);
  }

  public void freezeDatabase(String database, String user, String password) {
    OFreezeDatabaseRequest request = new OFreezeDatabaseRequest(database, null);
    OFreezeDatabaseResponse response = connectAndSend(database, user, password, request);
  }

  @Override
  public OResultSet executeServerStatement(
      String statement, String user, String pw, Object... params) {
    int recordsPerPage =
        getContextConfiguration()
            .getValueAsInteger(OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OServerQueryRequest request =
        new OServerQueryRequest(
            "sql",
            statement,
            params,
            OServerQueryRequest.COMMAND,
            ORecordSerializerNetworkV37Client.INSTANCE,
            recordsPerPage);

    OServerQueryResponse response = connectAndSend(null, user, pw, request);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            null,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());
    return rs;
  }

  @Override
  public OResultSet executeServerStatement(
      String statement, String user, String pw, Map<String, Object> params) {
    int recordsPerPage =
        getContextConfiguration()
            .getValueAsInteger(OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OServerQueryRequest request =
        new OServerQueryRequest(
            "sql",
            statement,
            params,
            OServerQueryRequest.COMMAND,
            ORecordSerializerNetworkV37Client.INSTANCE,
            recordsPerPage);

    OServerQueryResponse response = connectAndSend(null, user, pw, request);
    ORemoteResultSet rs =
        new ORemoteResultSet(
            null,
            response.getQueryId(),
            response.getResult(),
            response.getExecutionPlan(),
            response.getQueryStats(),
            response.isHasNextPage());

    return rs;
  }

  public OContextConfiguration getContextConfiguration() {
    return configurations.getConfigurations();
  }

  public <T extends OBinaryResponse> T networkAdminOperation(
      final OBinaryRequest<T> request, ORemoteClientSession session, final String errorMessage) {
    return networkAdminOperation(
        (network, nodeSession) -> {
          try {
            ORemoteClient.writeRequest(request, network.getChannelDataOutput(), nodeSession);
          } finally {
            network.endRequest();
          }
          T response = request.createResponse();
          try {
            ORemoteClient.beginResponse(network, nodeSession);
            response.read(network.getChannelDataInput());
          } finally {
            network.endResponse();
          }
          return response;
        },
        errorMessage,
        session);
  }

  public <T> T networkAdminOperation(
      final ORemoteClientOperation<T> operation,
      final String errorMessage,
      ORemoteClientSession session) {

    OChannelBinaryAsynchClient network = null;
    OContextConfiguration config = getContextConfiguration();
    try {
      String serverUrl =
          urls.getNextAvailableServerURL(false, session, config, CONNECTION_STRATEGY.STICKY);
      do {
        try {
          network = ORemoteClient.getNetwork(serverUrl, connectionManager, config);
        } catch (OException e) {
          serverUrl = urls.removeAndGet(serverUrl);
          if (serverUrl == null) throw e;
        }
      } while (network == null);
      ORemoteClientNodeSession nodeSession =
          session.getOrCreateServerSession(network.getServerURL());
      T res = operation.execute(network, nodeSession);
      connectionManager.release(network);
      return res;
    } catch (Exception e) {
      if (network != null) connectionManager.release(network);
      session.closeAllSessions(connectionManager, config);
      throw OException.wrapException(new OStorageException(errorMessage), e);
    }
  }

  private interface SessionOperation<T> {
    T execute(ORemoteClientSession session) throws IOException;
  }

  private <T> T connectAndExecute(
      String name, String user, String password, SessionOperation<T> operation) {
    checkOpen();
    ORemoteClientSession newSession = new ORemoteClientSession(-1);
    int retry = configurations.getConfigurations().getValueAsInteger(NETWORK_SOCKET_RETRY);
    while (retry > 0) {
      try {
        String url = buildUrl(name);
        connectAdminSession(user, password, newSession, url);

        T result = operation.execute(newSession);
        return result;
      } catch (IOException e) {
        retry--;
        if (retry == 0)
          throw OException.wrapException(
              new ODatabaseException(
                  "Reached maximum retry limit on admin operations, the server may be offline"),
              e);
      } finally {
        newSession.closeAllSessions(connectionManager, configurations.getConfigurations());
      }
    }
    // SHOULD NEVER REACH THIS POINT
    throw new ODatabaseException(
        "Reached maximum retry limit on admin operations, the server may be offline");
  }

  protected void connectAdminSession(
      String user, String password, ORemoteClientSession newSession, String url) {
    OCredentialInterceptor ci = OSecurityManager.instance().newCredentialInterceptor();

    String username;
    String foundPassword;
    if (ci != null) {
      ci.intercept(url, user, password);
      username = ci.getUsername();
      foundPassword = ci.getPassword();
    } else {
      username = user;
      foundPassword = password;
    }
    OConnect37Request request = new OConnect37Request(username, foundPassword);

    networkAdminOperation(
        (network, nodeSession) -> {
          try {
            ORemoteClient.writeRequest(request, network.getChannelDataOutput(), nodeSession);
          } finally {
            network.endRequest();
          }
          OConnectResponse response = request.createResponse();
          try {
            byte currentStatus = network.waitResponse();
            ORemoteClient.readResponseHeader(
                nodeSession.getSessionId(), currentStatus, network.getChannelDataInput());
            response.read(network.getChannelDataInput());
            nodeSession.setSession(response.getSessionId(), response.getSessionToken());
          } finally {
            network.endResponse();
          }
          return null;
        },
        "Cannot connect to the remote server/database '" + url + "'",
        newSession);
  }

  @Override
  public OrientDBConfig getConfigurations() {
    return configurations;
  }

  @Override
  public OSecuritySystem getSecuritySystem() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void create(
      String name,
      String user,
      String password,
      ODatabaseType type,
      ODatabaseId id,
      OrientDBConfig config,
      OCreateDatabaseParameters createParameters) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OAdminSession admin(String user, String password) {
    ORemoteClientSession newSession = new ORemoteClientSession(-1);
    connectAdminSession(user, password, newSession, buildUrl(null));
    return new ORemoteAdminSession(this, newSession);
  }

  @Override
  public String getConnectionUrl() {
    return "remote:" + String.join(ORemoteClient.ADDRESS_SEPARATOR, this.urls.getUrls());
  }

  @Override
  public ONodeId getNodeId() {
    return new ONodeId("$$remote_unknown_id");
  }
}
