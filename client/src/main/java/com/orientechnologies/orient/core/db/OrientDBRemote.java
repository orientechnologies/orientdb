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
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.ORemoteConnectionManager;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.NETWORK_LOCK_TIMEOUT;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBRemote implements OrientDBInternal {
  private final Map<String, OStorageRemote> storages = new HashMap<>();
  private final Set<ODatabasePoolInternal>  pools    = new HashSet<>();
  private final      String[]                 hosts;
  private final      OrientDBConfig           configurations;
  private final      Orient                   orient;
  protected volatile ORemoteConnectionManager connectionManager;
  private volatile boolean open = true;

  public OrientDBRemote(String[] hosts, OrientDBConfig configurations, Orient orient) {
    super();
    this.hosts = hosts;
    this.orient = orient;
    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();
    connectionManager = new ORemoteConnectionManager(this.configurations.getConfigurations().getValueAsLong(NETWORK_LOCK_TIMEOUT));
    orient.addOrientDB(this);
  }

  private String buildUrl(String name) {
    return String.join(",", hosts) + "/" + name;
  }

  public ODatabaseDocumentInternal open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  @Override
  public synchronized ODatabaseDocumentInternal open(String name, String user, String password, OrientDBConfig config) {
    checkOpen();
    try {
      OStorageRemote storage;
      storage = storages.get(name);
      if (storage == null) {
        storage = new OStorageRemote(buildUrl(name), this, "rw", connectionManager);
        storages.put(name, storage);
      }
      ODatabaseDocumentRemote db = new ODatabaseDocumentRemote(storage);
      db.internalOpen(user, password, solveConfig(config));
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
        storage = new OStorageRemote(buildUrl(name), this, "rw", connectionManager);
        storages.put(name, storage);
      } catch (Exception e) {
        throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
      }
    }
    ODatabaseDocumentRemotePooled db = new ODatabaseDocumentRemotePooled(pool, storage);
    db.internalOpen(user, password, pool.getConfig());
    return db;
  }

  public synchronized void closeStorage(OStorageRemote remote) {
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
    try {
      admin = new OServerAdmin(buildUrl(name));
      admin.connect(user, password);
      return operation.execute(admin);
    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("Error createing remote database "), e);
    } finally {
      if (admin != null)
        admin.close();
    }
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

  public void removePool(ODatabasePoolInternal pool) {
    pools.remove(pool);
  }

  @Override
  public synchronized void close() {
    if (!open)
      return;
    removeShutdownHook();
    internalClose();
  }

  public synchronized void internalClose() {
    if (!open)
      return;
    final List<OStorageRemote> storagesCopy = new ArrayList<>(storages.values());
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
    storages.clear();

    connectionManager.close();

    // SHUTDOWN ENGINES
    open = false;
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
  public void replaceFactory(OEmbeddedDatabaseInstanceFactory instanceFactory) {
    throw new UnsupportedOperationException("instance factory is not supported in remote");
  }

  @Override
  public void forceDatabaseClose(String databaseName) {
    throw new UnsupportedOperationException("force close is not supported in remote");
  }

  @Override
  public OEmbeddedDatabaseInstanceFactory getFactory() {
    throw new UnsupportedOperationException("instance factory is not supported in remote");
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

}
