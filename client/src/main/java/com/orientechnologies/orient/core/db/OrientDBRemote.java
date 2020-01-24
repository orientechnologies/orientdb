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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.ORemoteConnectionManager;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.OSharedContextRemote;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.orientechnologies.orient.client.remote.OStorageRemote.ADDRESS_SEPARATOR;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_LOCK_TIMEOUT;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_SOCKET_RETRY;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBRemote implements OrientDBInternal {
  protected final    Map<String, OSharedContext> sharedContexts = new HashMap<>();
  private final      Map<String, OStorageRemote> storages       = new HashMap<>();
  private final      Set<ODatabasePoolInternal>  pools          = new HashSet<>();
  private final      String[]                    hosts;
  private final      OrientDBConfig              configurations;
  private final      Orient                      orient;
  private final      OCachedDatabasePoolFactory  cachedPoolFactory;
  protected volatile ORemoteConnectionManager    connectionManager;
  private volatile   boolean                     open           = true;
  private            Timer                       timer;

  public OrientDBRemote(String[] hosts, OrientDBConfig configurations, Orient orient) {
    super();
    this.hosts = hosts;
    this.orient = orient;
    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();
    connectionManager = new ORemoteConnectionManager(this.configurations.getConfigurations().getValueAsLong(NETWORK_LOCK_TIMEOUT));
    orient.addOrientDB(this);
    timer = new Timer();
    cachedPoolFactory = createCachedDatabasePoolFactory(this.configurations);
  }

  protected OCachedDatabasePoolFactory createCachedDatabasePoolFactory(OrientDBConfig config) {
    int capacity = config.getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY);
    long timeout = config.getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT);
    return new OCachedDatabasePoolFactoryImpl(this, capacity, timeout);
  }

  private String buildUrl(String name) {
    return String.join(ADDRESS_SEPARATOR, hosts) + "/" + name;
  }

  public ODatabaseDocumentInternal open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  @Override
  public synchronized ODatabaseDocumentInternal open(String name, String user, String password, OrientDBConfig config) {
    checkOpen();
    OrientDBConfig resolvedConfig = solveConfig(config);
    try {
      OStorageRemote storage;
      storage = storages.get(name);
      if (storage == null) {
        storage = new OStorageRemote(buildUrl(name), this, "rw", connectionManager, resolvedConfig);
        storages.put(name, storage);
      }
      ODatabaseDocumentRemote db = new ODatabaseDocumentRemote(storage);
      db.internalOpen(user, password, resolvedConfig, getOrCreateSharedContext(storage));
      return db;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  public void create(String name, String user, String password, ODatabaseType databaseType) {
    create(name, user, password, databaseType, null);
  }

  @Override
  public synchronized void create(String name, String user, String password, ODatabaseType databaseType, OrientDBConfig config) {
    connectEndExecute(name, user, password, admin -> {
      String sendType = null;
      if (databaseType == ODatabaseType.MEMORY) {
        sendType = "memory";
      } else if (databaseType == ODatabaseType.PLOCAL) {
        sendType = "plocal";
      }
      admin.createDatabase(name, null, sendType);
      return null;
    });
  }

  public synchronized ODatabaseDocumentRemotePooled poolOpen(String name, String user, String password,
      ODatabasePoolInternal pool) {
    OStorageRemote storage = storages.get(name);
    if (storage == null) {
      try {
        storage = new OStorageRemote(buildUrl(name), this, "rw", connectionManager, solveConfig(pool.getConfig()));
        storages.put(name, storage);
      } catch (Exception e) {
        throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
      }
    }
    ODatabaseDocumentRemotePooled db = new ODatabaseDocumentRemotePooled(pool, storage);
    db.internalOpen(user, password, pool.getConfig(), getOrCreateSharedContext(storage));
    return db;
  }

  public synchronized void closeStorage(OStorageRemote remote) {
    OSharedContext ctx = sharedContexts.get(remote.getName());
    if (ctx != null) {
      ctx.close();
      sharedContexts.remove(remote.getName());
    }
    ODatabaseDocumentRemote.deInit(remote);
    storages.remove(remote.getName());
    remote.shutdown();
  }

  public ODocument getServerInfo(String username, String password) {
    return connectEndExecute(null, username, password, (admin) -> {
      return admin.getServerInfo();
    });
  }

  public ODocument getClusterStatus(String username, String password) {
    return connectEndExecute(null, username, password, (admin) -> {
      return admin.clusterStatus();
    });
  }

  public String getGlobalConfiguration(String username, String password, OGlobalConfiguration config) {
    return connectEndExecute(null, username, password, (admin) -> {
      return admin.getGlobalConfiguration(config);
    });
  }

  public void setGlobalConfiguration(String username, String password, OGlobalConfiguration config, String iConfigValue) {
    connectEndExecute(null, username, password, (admin) -> {
      admin.setGlobalConfiguration(config, iConfigValue);
      return null;
    });
  }

  public Map<String, String> getGlobalConfigurations(String username, String password) {
    return connectEndExecute(null, username, password, (admin) -> {
      return admin.getGlobalConfigurations();
    });
  }

  public ORemoteConnectionManager getConnectionManager() {
    return connectionManager;
  }

  private interface Operation<T> {
    T execute(OServerAdmin admin) throws IOException;
  }

  private <T> T connectEndExecute(String name, String user, String password, Operation<T> operation) {
    checkOpen();
    OServerAdmin admin = null;
    int retry = configurations.getConfigurations().getValueAsInteger(NETWORK_SOCKET_RETRY);
    while (retry > 0) {
      try {
        admin = new OServerAdmin(this, buildUrl(name));
        admin.connect(user, password);
        return operation.execute(admin);
      } catch (IOException e) {
        retry--;
        if (retry == 0)
          throw OException
              .wrapException(new ODatabaseException("Reached maximum retry limit on admin operations, the server may be offline"),
                  e);
      } finally {
        if (admin != null)
          admin.close();
      }
    }
    // SHOULD NEVER REACH THIS POINT
    throw new ODatabaseException("Reached maximum retry limit on admin operations, the server may be offline");
  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    return connectEndExecute(name, user, password, admin -> {
      // TODO: check for memory cases
      return admin.existsDatabase(name, null);
    });
  }

  @Override
  public synchronized void drop(String name, String user, String password) {
    connectEndExecute(name, user, password, admin -> {
      // TODO: check for memory cases
      return admin.dropDatabase(name, null);
    });

    OSharedContext ctx = sharedContexts.get(name);
    if (ctx != null) {
      ctx.close();
      sharedContexts.remove(name);
    }
    storages.remove(name);
  }

  @Override
  public Set<String> listDatabases(String user, String password) {
    return connectEndExecute("", user, password, admin -> {
      // TODO: check for memory cases
      return admin.listDatabases().keySet();
    });
  }

  @Override
  public void restore(String name, String user, String password, ODatabaseType type, String path, OrientDBConfig config) {
    connectEndExecute(name, user, password, admin -> {
      admin.createDatabase(name, "", type.name().toLowerCase(), path).close();
      return null;
    });

  }

  public ODatabasePoolInternal openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }

  @Override
  public ODatabasePoolInternal openPool(String name, String user, String password, OrientDBConfig config) {
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
  public ODatabasePoolInternal cachedPool(String database, String user, String password, OrientDBConfig config) {
    checkOpen();
    ODatabasePoolInternal pool = cachedPoolFactory.get(database, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  public void removePool(ODatabasePoolInternal pool) {
    pools.remove(pool);
  }

  @Override
  public void close() {
    if (!open)
      return;
    timer.cancel();
    removeShutdownHook();
    internalClose();
  }

  public void internalClose() {
    if (!open)
      return;

    final List<OStorageRemote> storagesCopy;
    synchronized (this) {
      // SHUTDOWN ENGINES AVOID OTHER OPENS
      open = false;
      this.sharedContexts.values().forEach(x -> x.close());
      storagesCopy = new ArrayList<>(storages.values());
    }

    for (OStorageRemote stg : storagesCopy) {
      try {
        ODatabaseDocumentRemote.deInit(stg);
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
    if (!open)
      throw new ODatabaseException("OrientDB Instance is closed");
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
    //In remote does nothing
  }

  @Override
  public ODatabaseDocumentInternal openNoAuthenticate(String iDbUrl, String user) {
    throw new UnsupportedOperationException("Open with no authentication is not supported in remote");
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
    if (remote != null)
      closeStorage(remote);
  }

  @Override
  public void restore(String name, InputStream in, Map<String, Object> options, Callable<Object> callable,
      OCommandOutputListener iListener) {
    throw new UnsupportedOperationException("raw restore is not supported in remote");
  }

  @Override
  public ODatabaseDocumentInternal openNoAuthorization(String name) {
    throw new UnsupportedOperationException("impossible skip authentication and authorization in remote");
  }

  protected synchronized OSharedContext getOrCreateSharedContext(OStorage storage) {

    OSharedContext result = sharedContexts.get(storage.getName());
    if (result == null) {
      result = createSharedContext(storage);
      sharedContexts.put(storage.getName(), result);
    }
    return result;
  }

  private OSharedContext createSharedContext(OStorage storage) {
    OSharedContextRemote result = new OSharedContextRemote(storage, this);
    storage.getResource(OSharedContext.class.getName(), () -> result);
    return result;

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
}
