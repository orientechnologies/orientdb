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
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBEmbedded implements OrientDBInternal {
  protected final Map<String, OAbstractPaginatedStorage> storages = new HashMap<>();
  protected final Set<ODatabasePoolInternal>             pools    = new HashSet<>();
  protected final    OrientDBConfig configurations;
  protected final    String         basePath;
  protected final    OEngine        memory;
  protected final    OEngine        disk;
  protected volatile Thread         shutdownThread;
  protected final    Orient         orient;
  private volatile boolean open = true;

  public OrientDBEmbedded(String directoryPath, OrientDBConfig configurations, Orient orient) {
    super();
    this.orient = orient;
    orient.onEmbeddedFactoryInit(this);
    memory = orient.getEngine("memory");
    disk = orient.getEngine("plocal");
    directoryPath = directoryPath.trim();
    if (directoryPath.length() != 0)
      this.basePath = new java.io.File(directoryPath).getAbsolutePath();
    else
      this.basePath = null;
    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();

    shutdownThread = new Thread(() -> OrientDBEmbedded.this.internalClose());

    Runtime.getRuntime().addShutdownHook(shutdownThread);

  }

  @Override
  public synchronized ODatabaseDocumentInternal open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  public synchronized ODatabaseDocumentInternal openNoAuthenticate(String name, String user) {
    try {
      OrientDBConfig config = solveConfig(null);
      OAbstractPaginatedStorage storage = getStorage(name);
      // THIS OPEN THE STORAGE ONLY THE FIRST TIME
      storage.open(config.getConfigurations());
      final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage);
      embedded.internalOpen(user, "nopwd", config, false);
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  public synchronized ODatabaseDocumentInternal open(String name, String user, String password, OrientDBConfig config) {
    checkOpen();
    try {
      config = solveConfig(config);
      OAbstractPaginatedStorage storage = getStorage(name);
      // THIS OPEN THE STORAGE ONLY THE FIRST TIME
      storage.open(config.getConfigurations());
      final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage);
      embedded.internalOpen(user, password, config);

      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  protected OrientDBConfig solveConfig(OrientDBConfig config) {
    if (config != null) {
      config.setParent(this.configurations);
      return config;
    } else {
      OrientDBConfig cfg = OrientDBConfig.defaultConfig();
      cfg.setParent(this.configurations);
      return cfg;
    }

  }

  public synchronized OEmbeddedDatabasePool poolOpen(String name, String user, String password, OEmbeddedPoolByFactory pool) {
    checkOpen();
    OAbstractPaginatedStorage storage = getStorage(name);
    storage.open(pool.getConfig().getConfigurations());
    final OEmbeddedDatabasePool embedded = new OEmbeddedDatabasePool(pool, storage);
    embedded.internalOpen(user, password, pool.getConfig());
    return embedded;
  }

  protected OAbstractPaginatedStorage getStorage(String name) {
    OAbstractPaginatedStorage storage = storages.get(name);
    if (storage == null) {
      storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(name), new HashMap<>());
      storages.put(name, storage);
    }
    return storage;
  }

  protected String buildName(String name) {
    if (basePath == null) {
      throw new ODatabaseException("OrientDB instanced created without physical path, only memory databases are allowed");
    }
    return basePath + "/" + name;
  }

  public void create(String name, String user, String password, ODatabaseType type) {
    create(name, user, password, type, null);
  }

  @Override
  public synchronized void create(String name, String user, String password, ODatabaseType type, OrientDBConfig config) {
    if (!exists(name, user, password)) {
      try {
        config = solveConfig(config);
        OAbstractPaginatedStorage storage;
        if (type == ODatabaseType.MEMORY) {
          storage = (OAbstractPaginatedStorage) memory.createStorage(name, new HashMap<>());
        } else {
          storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(name), new HashMap<>());
        }
        storages.put(name, storage);
        internalCreate(config, storage);
      } catch (Exception e) {
        throw OException.wrapException(new ODatabaseException("Cannot create database '" + name + "'"), e);
      }
    } else
      throw new ODatabaseException("Cannot create new storage '" + name + "' because it already exists");
  }

  public synchronized void restore(String name, String path) {
    if (!exists(name, null, null)) {
      try {
        OAbstractPaginatedStorage storage;
        storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(name), new HashMap<>());
        storage.restoreFromIncrementalBackup(path);
        storages.put(name, storage);
      } catch (Exception e) {
        throw OException.wrapException(new ODatabaseException("Cannot restore database '" + name + "'"), e);
      }
    } else
      throw new ODatabaseException("Cannot create new storage '" + name + "' because it already exists");
  }

  public synchronized void restore(String name, InputStream in, Map<String, Object> options, Callable<Object> callable,
      OCommandOutputListener iListener) {
    try {
      OAbstractPaginatedStorage storage = getStorage(name);
      storage.restore(in, options, callable, iListener);
      storages.put(name, storage);
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot create database '" + name + "'"), e);
    }
  }

  protected void internalCreate(OrientDBConfig config, OAbstractPaginatedStorage storage) {
    try {
      storage.create(config.getConfigurations());
    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("Error on database creation"), e);
    }

    ORecordSerializer serializer = ORecordSerializerFactory.instance().getDefaultRecordSerializer();
    if (serializer.toString().equals("ORecordDocument2csv"))
      throw new ODatabaseException("Impossible to create the database with ORecordDocument2csv serializer");
    storage.getConfiguration().setRecordSerializer(serializer.toString());
    storage.getConfiguration().setRecordSerializerVersion(serializer.getCurrentVersion());
    // since 2.1 newly created databases use strict SQL validation by default
    storage.getConfiguration().setProperty(OStatement.CUSTOM_STRICT_SQL, "true");

    storage.getConfiguration().update();

    // No need to close
    final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage);
    embedded.setSerializer(serializer);
    embedded.internalCreate(config);

  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    checkOpen();
    if (!storages.containsKey(name)) {
      if (basePath != null) {
        return OLocalPaginatedStorage.exists(Paths.get(buildName(name)));
      } else
        return false;
    }
    return true;
  }

  @Override
  public synchronized void drop(String name, String user, String password) {
    checkOpen();
    if (exists(name, user, password)) {
      getStorage(name).delete();
      storages.remove(name);
    }
  }

  protected interface DatabaseFound {
    void found(String name);
  }

  @Override
  public synchronized Set<String> listDatabases(String user, String password) {
    checkOpen();
    // SEARCH IN CONFIGURED PATHS
    final Set<String> databases = new HashSet<>();
    // SEARCH IN DEFAULT DATABASE DIRECTORY
    if (basePath != null) {
      scanDatabaseDirectory(new File(basePath), (name) -> databases.add(name));
    }
    databases.addAll(this.storages.keySet());
    // TODO remove OSystemDatabase.SYSTEM_DB_NAME from the list
    return databases;
  }

  public synchronized void loadAllDatabases() {
    if (basePath != null) {
      scanDatabaseDirectory(new File(basePath), (name) -> {
        if (!storages.containsKey(name)) {
          OAbstractPaginatedStorage storage = getStorage(name);
          // THIS OPEN THE STORAGE ONLY THE FIRST TIME
          storage.open(getConfigurations().getConfigurations());
        }
      });
    }
  }

  public ODatabasePoolInternal openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }

  @Override
  public ODatabasePoolInternal openPool(String name, String user, String password, OrientDBConfig config) {
    checkOpen();
    OEmbeddedPoolByFactory pool = new OEmbeddedPoolByFactory(this, name, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  @Override
  public synchronized void close() {
    removeShutdownHook();
    internalClose();
    open = false;
  }

  protected synchronized void internalClose() {
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

    orient.onEmbeddedFactoryClose(this);
  }

  public OrientDBConfig getConfigurations() {
    return configurations;
  }

  public void removePool(OEmbeddedPoolByFactory pool) {
    pools.remove(pool);
  }

  public interface InstanceFactory<T> {
    T create(OAbstractPaginatedStorage storage);
  }

  protected void scanDatabaseDirectory(final File directory, DatabaseFound found) {
    if (directory.exists() && directory.isDirectory()) {
      final File[] files = directory.listFiles();
      if (files != null)
        for (File db : files) {
          if (db.isDirectory()) {
            final File plocalFile = new File(db.getAbsolutePath() + "/database.ocf");
            final String dbPath = db.getPath().replace('\\', '/');
            final int lastBS = dbPath.lastIndexOf('/', dbPath.length() - 1) + 1;// -1 of dbPath may be ended with slash
            if (plocalFile.exists()) {
              found.found(OIOUtils.getDatabaseNameFromPath(dbPath.substring(lastBS)));
            }
          }
        }
    }
  }

  public synchronized void initCustomStorage(String name, String path, String userName, String userPassword) {
    boolean exists = OLocalPaginatedStorage.exists(Paths.get(path));
    OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) disk.createStorage(path, new HashMap<>());
    // TODO: Add Creation settings and parameters
    if (!exists) {
      internalCreate(getConfigurations(), storage);
    }
    storages.put(name, storage);

  }

  public synchronized void removeShutdownHook() {
    if (shutdownThread != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownThread);
      shutdownThread = null;
    }
  }

  public Collection<OAbstractPaginatedStorage> getStorages() {
    return storages.values();
  }

  public synchronized void forceDatabaseClose(String iDatabaseName) {
    OAbstractPaginatedStorage storage = storages.remove(iDatabaseName);
    if (storage != null) {
      storage.close();
    }
  }

  public String getDatabasePath(String iDatabaseName) {
    OAbstractPaginatedStorage storage = storages.get(iDatabaseName);
    if (storage != null && storage instanceof OLocalPaginatedStorage)
      return ((OLocalPaginatedStorage) storage).getStoragePath().toString();
    return null;
  }

  private void checkOpen() {
    if (!open)
      throw new ODatabaseException("OrientDB Instance is closed");
  }

}
