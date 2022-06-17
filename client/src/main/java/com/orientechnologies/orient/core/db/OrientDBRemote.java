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

package com.orientechnologies.orient.core.db;

import static com.orientechnologies.orient.client.remote.OStorageRemote.ADDRESS_SEPARATOR;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_SOCKET_RETRY;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.*;
import com.orientechnologies.orient.client.remote.OStorageRemote.CONNECTION_STRATEGY;
import com.orientechnologies.orient.client.remote.message.*;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.OSharedContextRemote;
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
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/** Created by tglman on 08/04/16. */
public class OrientDBRemote implements OrientDBInternal {
  protected final Map<String, OSharedContext> sharedContexts = new HashMap<>();
  private final Map<String, OStorageRemote> storages = new HashMap<>();
  private final Set<ODatabasePoolInternal> pools = new HashSet<>();
  private final String[] hosts;
  private final OrientDBConfig configurations;
  private final Orient orient;
  private final OCachedDatabasePoolFactory cachedPoolFactory;
  protected volatile ORemoteConnectionManager connectionManager;
  private volatile boolean open = true;
  private final Timer timer;
  private final ORemoteURLs urls;

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
      OStorageRemote storage;
      synchronized (this) {
        storage = storages.get(name);
        if (storage == null) {
          storage = new OStorageRemote(urls, name, this, "rw", connectionManager, resolvedConfig);
          storages.put(name, storage);
        }
      }
      ODatabaseDocumentRemote db =
          new ODatabaseDocumentRemote(storage, getOrCreateSharedContext(storage));
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
      OLogManager.instance().error(this, message, null);
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
    OStorageRemote storage;
    synchronized (this) {
      storage = storages.get(name);
      if (storage == null) {
        try {
          storage =
              new OStorageRemote(
                  urls, name, this, "rw", connectionManager, solveConfig(pool.getConfig()));
          storages.put(name, storage);
        } catch (Exception e) {
          throw OException.wrapException(
              new ODatabaseException("Cannot open database '" + name + "'"), e);
        }
      }
    }
    ODatabaseDocumentRemotePooled db =
        new ODatabaseDocumentRemotePooled(pool, storage, getOrCreateSharedContext(storage));
    db.internalOpen(user, password, pool.getConfig());
    return db;
  }

  public synchronized void closeStorage(OStorageRemote remote) {
    OSharedContext ctx = sharedContexts.get(remote.getName());
    if (ctx != null) {
      ctx.close();
      sharedContexts.remove(remote.getName());
    }
    storages.remove(remote.getName());
    remote.shutdown();
  }

  public ODocument getServerInfo(String username, String password) {
    OServerInfoRequest request = new OServerInfoRequest();
    OServerInfoResponse response = connectAndSend(null, username, password, request);
    ODocument res = new ODocument();
    res.fromJSON(response.getResult());

    return res;
  }

  public ODocument getClusterStatus(String username, String password) {
    ODistributedStatusRequest request = new ODistributedStatusRequest();
    ODistributedStatusResponse response = connectAndSend(null, username, password, request);

    OLogManager.instance()
        .debug(this, "Cluster status %s", response.getClusterConfig().toJSON("prettyPrint"));
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
    storages.remove(name);
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
      OLogManager.instance().error(this, message, null);
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
    timer.cancel();
    removeShutdownHook();
    internalClose();
  }

  public void internalClose() {
    if (!open) return;

    if (timer != null) {
      timer.cancel();
    }

    final List<OStorageRemote> storagesCopy;
    synchronized (this) {
      // SHUTDOWN ENGINES AVOID OTHER OPENS
      open = false;
      this.sharedContexts.values().forEach(x -> x.close());
      storagesCopy = new ArrayList<>(storages.values());
    }

    for (OStorageRemote stg : storagesCopy) {
      try {
        OLogManager.instance().info(this, "- shutdown storage: " + stg.getName() + "...");
        stg.shutdown();
      } catch (Exception e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
      } catch (Error e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
        throw e;
      }
    }
    synchronized (this) {
      this.sharedContexts.clear();
      storages.clear();

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
    OStorageRemote remote = storages.get(databaseName);
    if (remote != null) closeStorage(remote);
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

  protected synchronized OSharedContext getOrCreateSharedContext(OStorageRemote storage) {

    OSharedContext result = sharedContexts.get(storage.getName());
    if (result == null) {
      result = createSharedContext(storage);
      sharedContexts.put(storage.getName(), result);
    }
    return result;
  }

  private OSharedContext createSharedContext(OStorageRemote storage) {
    OSharedContextRemote context = new OSharedContextRemote(storage, this);
    storage.setSharedContext(context);
    return context;
  }

  public void schedule(TimerTask task, long delay, long period) {
    timer.schedule(task, delay, period);
  }

  public void scheduleOnce(TimerTask task, long delay) {
    timer.schedule(task, delay);
  }

  @Override
  public <X> Future<X> executeNoAuthorization(String database, ODatabaseTask<X> task) {
    throw new UnsupportedOperationException("execute with no session not available in remote");
  }

  @Override
  public <X> Future<X> execute(String database, String user, ODatabaseTask<X> task) {
    throw new UnsupportedOperationException("execute with no session not available in remote");
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
    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata())
        .getResult();
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

    return new ORemoteQueryResult(rs, response.isTxChanges(), response.isReloadMetadata())
        .getResult();
  }

  public OContextConfiguration getContextConfiguration() {
    return configurations.getConfigurations();
  }

  public <T extends OBinaryResponse> T networkAdminOperation(
      final OBinaryRequest<T> request, OStorageRemoteSession session, final String errorMessage) {
    return networkAdminOperation(
        new OStorageRemoteOperation<T>() {
          @Override
          public T execute(OChannelBinaryAsynchClient network, OStorageRemoteSession session)
              throws IOException {
            try {
              network.beginRequest(request.getCommand(), session);
              request.write(network, session);
            } finally {
              network.endRequest();
            }
            T response = request.createResponse();
            try {
              OStorageRemote.beginResponse(network, session);
              response.read(network, session);
            } finally {
              network.endResponse();
            }
            return response;
          }
        },
        errorMessage,
        session);
  }

  public <T> T networkAdminOperation(
      final OStorageRemoteOperation<T> operation,
      final String errorMessage,
      OStorageRemoteSession session) {

    OChannelBinaryAsynchClient network = null;
    OContextConfiguration config = getContextConfiguration();
    try {
      String serverUrl =
          urls.getNextAvailableServerURL(false, session, config, CONNECTION_STRATEGY.STICKY);
      do {
        try {
          network = OStorageRemote.getNetwork(serverUrl, connectionManager, config);
        } catch (OException e) {
          serverUrl = urls.removeAndGet(serverUrl);
          if (serverUrl == null) throw e;
        }
      } while (network == null);

      T res = operation.execute(network, session);
      connectionManager.release(network);
      return res;
    } catch (Exception e) {
      if (network != null) connectionManager.release(network);
      session.closeAllSessions(connectionManager, config);
      throw OException.wrapException(new OStorageException(errorMessage), e);
    }
  }

  private interface SessionOperation<T> {
    T execute(OStorageRemoteSession session) throws IOException;
  }

  private <T> T connectAndExecute(
      String name, String user, String password, SessionOperation<T> operation) {
    checkOpen();
    OStorageRemoteSession newSession = new OStorageRemoteSession(-1);
    int retry = configurations.getConfigurations().getValueAsInteger(NETWORK_SOCKET_RETRY);
    while (retry > 0) {
      try {
        OCredentialInterceptor ci = OSecurityManager.instance().newCredentialInterceptor();

        String username;
        String foundPassword;
        String url = buildUrl(name);
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
            (network, session) -> {
              OStorageRemoteNodeSession nodeSession =
                  session.getOrCreateServerSession(network.getServerURL());
              try {
                network.beginRequest(request.getCommand(), session);
                request.write(network, session);
              } finally {
                network.endRequest();
              }
              OConnectResponse response = request.createResponse();
              try {
                network.beginResponse(nodeSession.getSessionId(), true);
                response.read(network, session);
              } finally {
                network.endResponse();
              }
              return null;
            },
            "Cannot connect to the remote server/database '" + url + "'",
            newSession);

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
      OrientDBConfig config,
      ODatabaseTask<Void> createOps) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getConnectionUrl() {
    return "remote:" + String.join(OStorageRemote.ADDRESS_SEPARATOR, this.urls.getUrls());
  }
}
