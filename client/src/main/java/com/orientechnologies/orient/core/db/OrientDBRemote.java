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
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.util.*;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBRemote implements OrientDBInternal {
  private final Map<String, OStorageRemote> storages = new HashMap<>();
  private final Set<ODatabasePoolInternal>  pools    = new HashSet<>();
  private final String[]       hosts;
  private final OEngineRemote  remote;
  private final OrientDBConfig configurations;
  private final Thread         shutdownThread;
  private final Orient         orient;

  public OrientDBRemote(String[] hosts, OrientDBConfig configurations, Orient orient) {
    super();
    this.hosts = hosts;
    this.orient = orient;
    remote = (OEngineRemote) orient.getEngine("remote");

    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();

    shutdownThread = new Thread(() -> OrientDBRemote.this.internalClose());

    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }

  private String buildUrl(String name) {
    return hosts[0] + "/" + name;
  }

  public ODatabaseDocument open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  @Override
  public synchronized ODatabaseDocument open(String name, String user, String password, OrientDBConfig config) {
    try {
      OStorageRemote storage;
      storage = storages.get(name);
      if (storage == null) {
        storage = remote.createStorage(buildUrl(name), new HashMap<>());
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

  public synchronized ORemoteDatabasePool poolOpen(String name, String user, String password, ORemotePoolByFactory pool) {
    OStorageRemote storage = storages.get(name);
    if (storage == null) {
      storage = remote.createStorage(buildUrl(name), new HashMap<>());
    }
    ORemoteDatabasePool db = new ORemoteDatabasePool(pool, storage);
    db.internalOpen(user, password, pool.getConfig());
    return db;
  }

  private interface Operation<T> {
    T execute(OServerAdmin admin) throws IOException;
  }

  private <T> T connectEndExecute(String name, String user, String password, Operation<T> operation) {
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

  public ODatabasePoolInternal openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }

  @Override
  public ODatabasePoolInternal openPool(String name, String user, String password, OrientDBConfig config) {
    ORemotePoolByFactory pool = new ORemotePoolByFactory(this, name, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  public void removePool(ORemotePoolByFactory pool) {
    pools.remove(pool);
  }

  @Override
  public synchronized void close() {
    Runtime.getRuntime().removeShutdownHook(shutdownThread);
    internalClose();
  }

  public synchronized void internalClose() {
    final List<OStorage> storagesCopy = new ArrayList<OStorage>(storages.values());
    for (OStorage stg : storagesCopy) {
      try {
        OLogManager.instance().info(this, "- shutdown storage: " + stg.getName() + "...");
        stg.shutdown();
      } catch (Throwable e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
      }
    }
    storages.clear();

    // SHUTDOWN ENGINES
    remote.shutdown();
  }

  private OrientDBConfig solveConfig(OrientDBConfig config) {
    if (config != null) {
      config.setParent(this.configurations);
      return config;
    } else
      return this.configurations;
  }

  public OrientDBConfig getConfigurations() {
    return configurations;
  }
}
