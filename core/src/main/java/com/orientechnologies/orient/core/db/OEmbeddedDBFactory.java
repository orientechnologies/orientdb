/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import java.io.File;
import java.util.*;

/**
 * Created by tglman on 08/04/16.
 */
public class OEmbeddedDBFactory implements OrientDBFactory {
  private final Map<String, OAbstractPaginatedStorage> storages = new HashMap<>();
  private final Set<OPool<?>>                          pools    = new HashSet<>();
  private final OrientDBConfig                       configurations;
  private final String                                 basePath;
  private final OEngine                                memory;
  private final OEngine                                disk;
  private final Thread                                 shutdownThread;

  public OEmbeddedDBFactory(String directoryPath, OrientDBConfig configurations) {
    super();

    memory = Orient.instance().getEngine("memory");
    memory.startup();
    disk = Orient.instance().getEngine("plocal");
    disk.startup();

    this.basePath = new java.io.File(directoryPath).getAbsolutePath();
    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();

    try {
      if (OByteBufferPool.instance() != null)
        OByteBufferPool.instance().registerMBean();
    } catch (Exception e) {
      OLogManager.instance().error(this, "MBean for byte buffer pool cannot be registered", e);
    }

    shutdownThread = new Thread(() -> OEmbeddedDBFactory.this.internalClose());

    Runtime.getRuntime().addShutdownHook(shutdownThread);

  }

  @Override
  public synchronized ODatabaseDocument open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  @Override
  public ODatabaseDocument open(String name, String user, String password, OrientDBConfig config) {
    try {
      config = solveConfig(config);
      OAbstractPaginatedStorage storage = getStorage(name);
      storage.open(config.getConfigurations());
      final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage);
      embedded.internalOpen(user, password, config);

      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  private OrientDBConfig solveConfig(OrientDBConfig config) {
    if (config != null) {
      config.setParent(this.configurations);
      return config;
    } else
      return this.configurations;
  }

  public synchronized OEmbeddedDatabasePool poolOpen(String name, String user, String password, OEmbeddedPoolByFactory pool) {
    OAbstractPaginatedStorage storage = getStorage(name);
    storage.open(pool.getConfig().getConfigurations());
    final OEmbeddedDatabasePool embedded = new OEmbeddedDatabasePool(pool, storage);
    embedded.internalOpen(user, password, pool.getConfig());
    return embedded;
  }

  private OAbstractPaginatedStorage getStorage(String name) {
    OAbstractPaginatedStorage storage = storages.get(name);
    if (storage == null) {
      storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(name), new HashMap<>());
      storages.put("storage", storage);
    }
    return storage;
  }

  private String buildName(String name) {
    return basePath + "/" + name;
  }

  public void create(String name, String user, String password, DatabaseType type){
    create(name, user, password, type, null);
  }
  
  @Override
  public synchronized void create(String name, String user, String password, DatabaseType type, OrientDBConfig config) {
    if (!exists(name, user, password)) {
      try {
        config = solveConfig(config);
        OAbstractPaginatedStorage storage;
        if (type == DatabaseType.MEMORY) {
          storage = (OAbstractPaginatedStorage) memory.createStorage(buildName(name), new HashMap<>());
        } else {
          storage = (OAbstractPaginatedStorage) disk.createStorage(buildName(name), new HashMap<>());
        }
        // CHECK Configurations
        storage.create(config.getConfigurations());
        storages.put(name, storage);
        ORecordSerializer serializer = ORecordSerializerFactory.instance().getDefaultRecordSerializer();
        storage.getConfiguration().setRecordSerializer(serializer.toString());
        storage.getConfiguration().setRecordSerializerVersion(serializer.getCurrentVersion());
        // since 2.1 newly created databases use strict SQL validation by default
        storage.getConfiguration().setProperty(OStatement.CUSTOM_STRICT_SQL, "true");

        storage.getConfiguration().update();

        try (final ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage)) {
          embedded.internalCreate(config);
        }
      } catch (Exception e) {
        throw OException.wrapException(new ODatabaseException("Cannot create database '" + name + "'"), e);
      }
    } else
      throw new OStorageExistsException("Cannot create new storage '" + name + "' because it already exists");
  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    if (!storages.containsKey(name)) {
      return OLocalPaginatedStorage.exists(buildName(name));
    }
    return true;
  }

  @Override
  public synchronized void drop(String name, String user, String password) {
    if (exists(name, user, password)) {
      getStorage(name).delete();
      storages.remove(name);
    }
  }

  @Override
  public Set<String> listDatabases(String user, String password) {
    // SEARCH IN CONFIGURED PATHS
    final Set<String> databases = new HashSet<>();
    // SEARCH IN DEFAULT DATABASE DIRECTORY
    final String rootDirectory = basePath;
    scanDatabaseDirectory(new File(rootDirectory), databases);
    databases.addAll(this.storages.keySet());

    // TODO remove OSystemDatabase.SYSTEM_DB_NAME from the list
    return databases;
  }

  public OPool<ODatabaseDocument> openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }
  
  @Override
  public OPool<ODatabaseDocument> openPool(String name, String user, String password,OrientDBConfig config) {
    OEmbeddedPoolByFactory pool = new OEmbeddedPoolByFactory(this, name, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  @Override
  public synchronized void close() {
    internalClose();
    Runtime.getRuntime().removeShutdownHook(shutdownThread);
  }

  private synchronized void internalClose() {
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
    memory.shutdown();
    disk.shutdown();
  }

  public OrientDBConfig getConfigurations() {
    return configurations;
  }

  public /* OServer */ Object spawnServer(/* OServerConfiguration */Object serverConfiguration) {
    return null;
  }

  public void removePool(OEmbeddedPoolByFactory pool) {
    pools.remove(pool);
  }

  public interface InstanceFactory<T> {
    T create(OAbstractPaginatedStorage storage);
  }

  private void scanDatabaseDirectory(final File directory, final Set<String> storages) {
    if (directory.exists() && directory.isDirectory()) {
      final File[] files = directory.listFiles();
      if (files != null)
        for (File db : files) {
          if (db.isDirectory()) {
            final File plocalFile = new File(db.getAbsolutePath() + "/database.ocf");
            final String dbPath = db.getPath().replace('\\', '/');
            final int lastBS = dbPath.lastIndexOf('/', dbPath.length() - 1) + 1;// -1 of dbPath may be ended with slash
            if (plocalFile.exists()) {
              storages.add(OIOUtils.getDatabaseNameFromPath(dbPath.substring(lastBS)));
            }
          }
        }
    }
  }

}
