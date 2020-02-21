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
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.engine.OMemoryAndLocalPaginatedEnginesInitializer;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_RETRY;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBEmbedded implements OrientDBInternal {
  /**
   * Next storage id
   */
  private static final AtomicInteger nextStorageId = new AtomicInteger();

  protected final  Map<String, OAbstractPaginatedStorage> storages       = new HashMap<>();
  protected final  Set<ODatabasePoolInternal>             pools          = new HashSet<>();
  protected final  OrientDBConfig                         configurations;
  protected final  String                                 basePath;
  protected final  OEngine                                memory;
  protected final  OEngine                                disk;
  protected final  Orient                                 orient;
  private volatile boolean                                open           = true;
  private volatile OEmbeddedDatabaseInstanceFactory       factory        = new ODefaultEmbeddedDatabaseInstanceFactory();
  private          TimerTask                              autoCloseTimer = null;

  protected final long maxWALSegmentSize;

  public OrientDBEmbedded(String directoryPath, OrientDBConfig configurations, Orient orient) {
    super();
    this.orient = orient;
    orient.onEmbeddedFactoryInit(this);
    memory = orient.getEngine("memory");
    disk = orient.getEngine("plocal");
    directoryPath = directoryPath.trim();
    if (directoryPath.length() != 0) {
      final File dirFile = new File(directoryPath);
      if (!dirFile.exists()) {
        OLogManager.instance().infoNoDb(this, "Directory " + dirFile + " does not exist, try to create it.");

        if (!dirFile.mkdirs()) {
          OLogManager.instance().errorNoDb(this, "Can not create directory " + dirFile, null);
        }
      }
      this.basePath = dirFile.getAbsolutePath();
    } else {
      this.basePath = null;
    }

    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();

    if (basePath == null) {
      maxWALSegmentSize = -1;
    } else {
      try {
        maxWALSegmentSize = calculateInitialMaxWALSegSize(configurations);

        if (maxWALSegmentSize <= 0) {
          throw new ODatabaseException("Invalid configuration settings. Can not set maximum size of WAL segment");
        }

        OLogManager.instance().infoNoDb(this, "WAL maximum segment size is set to %,d MB", maxWALSegmentSize / 1024 / 1024);
      } catch (IOException e) {
        throw OException.wrapException(new ODatabaseException("Cannot initialize OrientDB engine"), e);
      }
    }

    OMemoryAndLocalPaginatedEnginesInitializer.INSTANCE.initialize();

    orient.addOrientDB(this);
    boolean autoClose = this.configurations.getConfigurations().getValueAsBoolean(OGlobalConfiguration.AUTO_CLOSE_AFTER_DELAY);
    if (autoClose) {
      int autoCloseDelay = this.configurations.getConfigurations().getValueAsInteger(OGlobalConfiguration.AUTO_CLOSE_DELAY);
      final long delay = autoCloseDelay * 60 * 1000;
      initAutoClose(delay);
    }
  }

  public void initAutoClose(long delay) {
    final long scheduleTime = delay / 3;
    autoCloseTimer = orient.scheduleTask(() -> orient.submit(() -> checkAndCloseStorages(delay)), scheduleTime, scheduleTime);
  }

  private synchronized void checkAndCloseStorages(long delay) {
    Set<String> toClose = new HashSet<>();
    for (OAbstractPaginatedStorage storage : storages.values()) {
      if (storage.getType().equalsIgnoreCase(ODatabaseType.PLOCAL.name()) && storage.getSessionCount() == 0) {
        long currentTime = System.currentTimeMillis();
        if (currentTime > storage.getLastCloseTime() + delay) {
          toClose.add(storage.getName());
        }
      }
    }
    for (String storage : toClose) {
      forceDatabaseClose(storage);
    }
  }

  private long calculateInitialMaxWALSegSize(OrientDBConfig configurations) throws IOException {
    String walPath;

    if (configurations != null) {
      final OContextConfiguration config = configurations.getConfigurations();

      if (config != null) {
        walPath = config.getValueAsString(OGlobalConfiguration.WAL_LOCATION);
      } else {
        walPath = OGlobalConfiguration.WAL_LOCATION.getValueAsString();
      }
    } else {
      walPath = OGlobalConfiguration.WAL_LOCATION.getValueAsString();
    }

    if (walPath == null) {
      walPath = basePath;
    }

    final FileStore fileStore = Files.getFileStore(Paths.get(walPath));
    final long freeSpace = fileStore.getUsableSpace();

    final long filesSize = Files.walk(Paths.get(walPath)).mapToLong(p -> p.toFile().isFile() ? p.toFile().length() : 0).sum();
    long maxSegSize;

    if (configurations != null) {
      final OContextConfiguration config = configurations.getConfigurations();
      if (config != null) {
        maxSegSize = config.getValueAsLong(OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE) * 1024 * 1024;
      } else {
        maxSegSize = OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsLong() * 1024 * 1024;
      }
    } else {
      maxSegSize = OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE.getValueAsLong() * 1024 * 1024;
    }

    if (maxSegSize <= 0) {
      int sizePercent;
      if (configurations != null) {
        final OContextConfiguration config = configurations.getConfigurations();

        if (config != null) {
          sizePercent = config.getValueAsInteger(OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE_PERCENT);
        } else {
          sizePercent = OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE_PERCENT.getValueAsInteger();
        }
      } else {
        sizePercent = OGlobalConfiguration.WAL_MAX_SEGMENT_SIZE_PERCENT.getValueAsInteger();
      }

      if (sizePercent <= 0) {
        throw new ODatabaseException("Invalid configuration settings. Can not set maximum size of WAL segment");
      }

      maxSegSize = (freeSpace + filesSize) / 100 * sizePercent;
    }

    final long minSegSizeLimit = (long) (freeSpace * 0.25);

    long minSegSize = 0;
    if (configurations != null) {
      OContextConfiguration config = configurations.getConfigurations();
      if (config != null) {
        minSegSize = config.getValueAsLong(OGlobalConfiguration.WAL_MIN_SEG_SIZE) * 1024 * 1024;
      }
    }

    if (minSegSize <= 0) {
      minSegSize = OGlobalConfiguration.WAL_MIN_SEG_SIZE.getValueAsLong() * 1024 * 1024;
    }

    if (minSegSize > minSegSizeLimit) {
      minSegSize = minSegSizeLimit;
    }

    if (minSegSize > 0 && maxSegSize < minSegSize) {
      maxSegSize = minSegSize;
    }
    return maxSegSize;
  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  public ODatabaseDocumentEmbedded openNoAuthenticate(String name, String user) {
    try {
      final ODatabaseDocumentEmbedded embedded;
      OrientDBConfig config = solveConfig(null);
      synchronized (this) {
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        // THIS OPEN THE STORAGE ONLY THE FIRST TIME
        storage.open(config.getConfigurations());
        storage.incOnOpen();
        embedded = factory.newInstance(storage);
        embedded.init(config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(user, "nopwd", false);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  public ODatabaseDocumentEmbedded openNoAuthorization(String name) {
    try {
      final ODatabaseDocumentEmbedded embedded;
      OrientDBConfig config = solveConfig(null);
      synchronized (this) {
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        // THIS OPEN THE STORAGE ONLY THE FIRST TIME
        storage.open(config.getConfigurations());
        storage.incOnOpen();
        embedded = factory.newInstance(storage);
        embedded.init(config);
      }
      embedded.rebuildIndexes();
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password, OrientDBConfig config) {
    try {
      final ODatabaseDocumentEmbedded embedded;
      synchronized (this) {
        checkOpen();
        config = solveConfig(config);
        final OAbstractPaginatedStorage storage;
        storage = getOrInitStorage(name);
        try {
          // THIS OPEN THE STORAGE ONLY THE FIRST TIME
          storage.open(config.getConfigurations());
        } catch (RuntimeException e) {
          if (storage != null) {
            storages.remove(storage.getName());
          } else {
            storages.remove(name);
          }

          throw e;
        }
        storage.incOnOpen();
        embedded = factory.newInstance(storage);
        embedded.init(config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(user, password);
      embedded.callOnOpenListeners();
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

  public ODatabaseDocumentInternal poolOpen(String name, String user, String password, ODatabasePoolInternal pool) {
    final ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      checkOpen();
      OAbstractPaginatedStorage storage = getOrInitStorage(name);
      storage.open(pool.getConfig().getConfigurations());
      storage.incOnOpen();
      embedded = factory.newPoolInstance(pool, storage);
      embedded.init(pool.getConfig());
    }
    embedded.rebuildIndexes();
    embedded.internalOpen(user, password);
    embedded.callOnOpenListeners();
    return embedded;
  }

  protected OAbstractPaginatedStorage getOrInitStorage(String name) {
    OAbstractPaginatedStorage storage = storages.get(name);
    if (storage == null) {
      Path storagePath = Paths.get(buildName(name));
      if (OLocalPaginatedStorage.exists(storagePath)) {
        name = storagePath.getFileName().toString();
      }

      storage = storages.get(name);
      if (storage == null) {
        storage = (OAbstractPaginatedStorage) disk
            .createStorage(buildName(name), new HashMap<>(), maxWALSegmentSize, generateStorageId());
        if (storage.exists()) {
          storages.put(name, storage);
        }
      }
    }
    return storage;
  }

  protected final int generateStorageId() {
    return Math.abs(nextStorageId.getAndIncrement());
  }

  public synchronized OAbstractPaginatedStorage getStorage(String name) {
    return storages.get(name);
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
  public void create(String name, String user, String password, ODatabaseType type, OrientDBConfig config) {
    final ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      if (!exists(name, user, password)) {
        try {
          config = solveConfig(config);
          OAbstractPaginatedStorage storage;

          final int storageId = generateStorageId();
          if (type == ODatabaseType.MEMORY) {
            storage = (OAbstractPaginatedStorage) memory.createStorage(name, new HashMap<>(), maxWALSegmentSize, storageId);
          } else {
            storage = (OAbstractPaginatedStorage) disk
                .createStorage(buildName(name), new HashMap<>(), maxWALSegmentSize, storageId);
          }
          storages.put(name, storage);
          embedded = internalCreate(config, storage);
        } catch (Exception e) {
          throw OException.wrapException(new ODatabaseException("Cannot create database '" + name + "'"), e);
        }
      } else
        throw new ODatabaseException("Cannot create new database '" + name + "' because it already exists");
    }
    embedded.callOnCreateListeners();
    ODatabaseRecordThreadLocal.instance().remove();
  }

  public void restore(String name, String user, String password, ODatabaseType type, String path, OrientDBConfig config) {
    config = solveConfig(config);
    final ODatabaseDocumentEmbedded embedded;
    OAbstractPaginatedStorage storage;
    synchronized (this) {
      if (!exists(name, null, null)) {
        try {
          storage = (OAbstractPaginatedStorage) disk
              .createStorage(buildName(name), new HashMap<>(), maxWALSegmentSize, generateStorageId());
          embedded = internalCreate(config, storage);
          storages.put(name, storage);
        } catch (Exception e) {
          throw OException.wrapException(new ODatabaseException("Cannot restore database '" + name + "'"), e);
        }
      } else
        throw new ODatabaseException("Cannot create new storage '" + name + "' because it already exists");
    }
    storage.restoreFromIncrementalBackup(path);
    embedded.callOnCreateListeners();
    ODatabaseRecordThreadLocal.instance().remove();
  }

  public void restore(String name, InputStream in, Map<String, Object> options, Callable<Object> callable,
      OCommandOutputListener iListener) {
    try {
      OAbstractPaginatedStorage storage;
      synchronized (this) {
        storage = getOrInitStorage(name);
        storages.put(name, storage);
      }
      storage.restore(in, options, callable, iListener);
    } catch (Exception e) {
      OContextConfiguration configs = getConfigurations().getConfigurations();
      OLocalPaginatedStorage
          .deleteFilesFromDisc(name, configs.getValueAsInteger(FILE_DELETE_RETRY), configs.getValueAsInteger(FILE_DELETE_DELAY),
              buildName(name));
      throw OException.wrapException(new ODatabaseException("Cannot create database '" + name + "'"), e);
    }
  }

  protected ODatabaseDocumentEmbedded internalCreate(OrientDBConfig config, OAbstractPaginatedStorage storage) {
    storage.create(config.getConfigurations());

    ORecordSerializer serializer = ORecordSerializerFactory.instance().getDefaultRecordSerializer();
    if (serializer.toString().equals("ORecordDocument2csv"))
      throw new ODatabaseException("Impossible to create the database with ORecordDocument2csv serializer");
    storage.setRecordSerializer(serializer.toString(), serializer.getCurrentVersion());
    // since 2.1 newly created databases use strict SQL validation by default
    storage.setProperty(OStatement.CUSTOM_STRICT_SQL, "true");

    // No need to close
    final ODatabaseDocumentEmbedded embedded = factory.newInstance(storage);
    embedded.setSerializer(serializer);
    embedded.internalCreate(config);
    return embedded;
  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    checkOpen();
    OStorage storage = storages.get(name);
    if (storage == null) {
      if (basePath != null) {
        return OLocalPaginatedStorage.exists(Paths.get(buildName(name)));
      } else
        return false;
    }
    return storage.exists();
  }

  @Override
  public void drop(String name, String user, String password) {
    synchronized (this) {
      checkOpen();
    }
    ODatabaseDocumentInternal db = openNoAuthenticate(name, user);
    for (Iterator<ODatabaseLifecycleListener> it = orient.getDbLifecycleListeners(); it.hasNext(); ) {
      it.next().onDrop(db);
    }
    db.close();
    synchronized (this) {
      if (exists(name, user, password)) {
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        ODatabaseDocumentEmbedded.deInit(storage);
        storage.delete();
        storages.remove(name);
      }
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
      scanDatabaseDirectory(new File(basePath), databases::add);
    }
    databases.addAll(this.storages.keySet());
    // TODO remove OSystemDatabase.SYSTEM_DB_NAME from the list
    return databases;
  }

  public synchronized void loadAllDatabases() {
    if (basePath != null) {
      scanDatabaseDirectory(new File(basePath), (name) -> {
        if (!storages.containsKey(name)) {
          OAbstractPaginatedStorage storage = getOrInitStorage(name);
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
    ODatabasePoolImpl pool = new ODatabasePoolImpl(this, name, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  @Override
  public synchronized void close() {
    if (!open) {
      return;
    }
    removeShutdownHook();
    internalClose();
  }

  public synchronized void internalClose() {
    if (!open)
      return;
    final List<OAbstractPaginatedStorage> storagesCopy = new ArrayList<>(storages.values());
    for (OAbstractPaginatedStorage stg : storagesCopy) {
      try {
        OLogManager.instance().info(this, "- shutdown storage: " + stg.getName() + "...");
        ODatabaseDocumentEmbedded.deInit(stg);
        stg.shutdown();
      } catch (Exception e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
      } catch (Error e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
        throw e;
      }
    }
    storages.clear();
    orient.onEmbeddedFactoryClose(this);
    if (autoCloseTimer != null) {
      autoCloseTimer.cancel();
    }
    open = false;
  }

  public OrientDBConfig getConfigurations() {
    return configurations;
  }

  public void removePool(ODatabasePoolInternal pool) {
    pools.remove(pool);
  }

  public interface InstanceFactory<T> {
    T create(OAbstractPaginatedStorage storage);
  }

  private static void scanDatabaseDirectory(final File directory, DatabaseFound found) {
    try {
      if (directory.exists() && directory.isDirectory()) {
        final File[] files = directory.listFiles();
        if (files != null)
          for (File db : files) {
            if (db.isDirectory()) {
              final Path dbPath = Paths.get(db.getAbsolutePath());
              try (DirectoryStream<Path> stream = Files.newDirectoryStream(dbPath)) {
                stream.forEach((p) -> {
                  if (!Files.isDirectory(p)) {
                    final String fileName = p.getFileName().toString();

                    if (fileName.equals("database.ocf") || (fileName.startsWith(OClusterBasedStorageConfiguration.COMPONENT_NAME)
                        && fileName.endsWith(OClusterBasedStorageConfiguration.DATA_FILE_EXTENSION))) {
                      final int count = p.getNameCount();
                      found.found(OIOUtils.getDatabaseNameFromPath(p.subpath(count - 2, count - 1).toString()));
                    }
                  }
                });
              }
            }
          }
      }
    } catch (IOException e) {
      throw OException.wrapException(new ODatabaseException("Exception during scanning of database directory"), e);
    }
  }

  public synchronized void initCustomStorage(String name, String path, String userName, String userPassword) {
    ODatabaseDocumentEmbedded embedded = null;
    synchronized (this) {
      boolean exists = OLocalPaginatedStorage.exists(Paths.get(path));
      OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) disk
          .createStorage(path, new HashMap<>(), maxWALSegmentSize, generateStorageId());
      // TODO: Add Creation settings and parameters
      if (!exists) {
        embedded = internalCreate(getConfigurations(), storage);
      }
      storages.put(name, storage);
    }
    if (embedded != null) {
      embedded.callOnCreateListeners();
      ODatabaseRecordThreadLocal.instance().remove();
    }
  }

  public synchronized void removeShutdownHook() {
    orient.removeOrientDB(this);
  }

  public synchronized Collection<OStorage> getStorages() {
    return storages.values().stream().map((x) -> (OStorage) x).collect(Collectors.toSet());
  }

  public synchronized void forceDatabaseClose(String iDatabaseName) {
    OAbstractPaginatedStorage storage = storages.remove(iDatabaseName);
    if (storage != null) {
      ODatabaseDocumentEmbedded.deInit(storage);
      storage.shutdown();
    }
  }

  public String getDatabasePath(String iDatabaseName) {
    OAbstractPaginatedStorage storage = storages.get(iDatabaseName);
    if (storage instanceof OLocalPaginatedStorage) {
      return ((OLocalPaginatedStorage) storage).getStoragePath().toString();
    }
    return null;
  }

  private void checkOpen() {
    if (!open)
      throw new ODatabaseException("OrientDB Instance is closed");
  }

  public synchronized void replaceFactory(OEmbeddedDatabaseInstanceFactory factory) {
    this.factory = factory;
  }

  public OEmbeddedDatabaseInstanceFactory getFactory() {
    return factory;
  }

  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

}
