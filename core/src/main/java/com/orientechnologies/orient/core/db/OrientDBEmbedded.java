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
import com.orientechnologies.orient.core.exception.OStorageException;
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
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_RETRY;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBEmbedded implements OrientDBInternal {
  /**
   * Keeps track of next possible storage id.
   */
  private static final AtomicInteger nextStorageId     = new AtomicInteger();
  /**
   * Storage IDs current assigned to the storage.
   */
  private static final Set<Integer>  currentStorageIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

  protected final  Map<String, OAbstractPaginatedStorage> storages       = new HashMap<>();
  protected final  Map<String, OSharedContext>            sharedContexts = new HashMap<>();
  protected final  Set<ODatabasePoolInternal>             pools          = new HashSet<>();
  protected final  OrientDBConfig                         configurations;
  protected final  String                                 basePath;
  protected final  OEngine                                memory;
  protected final  OEngine                                disk;
  protected final  Orient                                 orient;
  protected final  OCachedDatabasePoolFactory             cachedPoolFactory;
  private volatile boolean                                open           = true;
  private          ExecutorService                        executor;
  private          Timer                                  timer;
  private          TimerTask                              autoCloseTimer = null;

  protected final long maxWALSegmentSize;
  protected final long doubleWriteLogMaxSegSize;

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
      doubleWriteLogMaxSegSize = -1;
    } else {
      try {
        doubleWriteLogMaxSegSize = calculateDoubleWriteLogMaxSegSize(Paths.get(basePath));
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

    executor = new ThreadPoolExecutor(1, Runtime.getRuntime().availableProcessors(), 30, TimeUnit.MINUTES,
        new LinkedBlockingQueue<>());
    timer = new Timer();

    cachedPoolFactory = createCachedDatabasePoolFactory(this.configurations);

    boolean autoClose = this.configurations.getConfigurations().getValueAsBoolean(OGlobalConfiguration.AUTO_CLOSE_AFTER_DELAY);
    if (autoClose) {
      int autoCloseDelay = this.configurations.getConfigurations().getValueAsInteger(OGlobalConfiguration.AUTO_CLOSE_DELAY);
      final long delay = autoCloseDelay * 60 * 1000;
      initAutoClose(delay);
    }
  }

  protected OCachedDatabasePoolFactory createCachedDatabasePoolFactory(OrientDBConfig config) {
    int capacity = config.getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY);
    long timeout = config.getConfigurations().getValueAsInteger(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT);
    return new OCachedDatabasePoolFactoryImpl(this, capacity, timeout);
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

    long filesSize;
    try {
      filesSize = Files.walk(Paths.get(walPath)).mapToLong(p -> {
        try {
          if (Files.isRegularFile(p)) {
            return Files.size(p);
          }

          return 0;
        } catch (IOException | UncheckedIOException e) {
          OLogManager.instance().error(this, "Error during calculation of free space for database", e);
          return 0;
        }
      }).sum();
    } catch (IOException | UncheckedIOException e) {
      OLogManager.instance().error(this, "Error during calculation of free space for database", e);

      filesSize = 0;
    }

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

  private long calculateDoubleWriteLogMaxSegSize(Path storagePath) throws IOException {
    final FileStore fileStore = Files.getFileStore(storagePath);
    final long freeSpace = fileStore.getUsableSpace();

    long filesSize;
    try {
      filesSize = Files.walk(storagePath).mapToLong(p -> {
        try {
          if (Files.isRegularFile(p)) {
            return Files.size(p);
          }

          return 0;
        } catch (IOException | UncheckedIOException e) {
          OLogManager.instance().error(this, "Error during calculation of free space for database", e);

          return 0;
        }
      }).sum();
    } catch (IOException | UncheckedIOException e) {
      OLogManager.instance().error(this, "Error during calculation of free space for database", e);

      filesSize = 0;
    }

    long maxSegSize;

    if (configurations != null) {
      final OContextConfiguration config = configurations.getConfigurations();
      if (config != null) {
        maxSegSize = config.getValueAsLong(OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE) * 1024 * 1024;
      } else {
        maxSegSize = OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE.getValueAsLong() * 1024 * 1024;
      }
    } else {
      maxSegSize = OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE.getValueAsLong() * 1024 * 1024;
    }

    if (maxSegSize <= 0) {
      int sizePercent;
      if (configurations != null) {
        final OContextConfiguration config = configurations.getConfigurations();

        if (config != null) {
          sizePercent = config.getValueAsInteger(OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE_PERCENT);
        } else {
          sizePercent = OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE_PERCENT.getValueAsInteger();
        }
      } else {
        sizePercent = OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MAX_SEG_SIZE_PERCENT.getValueAsInteger();
      }

      if (sizePercent <= 0) {
        throw new ODatabaseException("Invalid configuration settings. Can not set maximum size of WAL segment");
      }

      maxSegSize = (freeSpace + filesSize) / 100 * sizePercent;
    }

    long minSegSize = 0;
    if (configurations != null) {
      OContextConfiguration config = configurations.getConfigurations();
      if (config != null) {
        minSegSize = config.getValueAsLong(OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MIN_SEG_SIZE) * 1024 * 1024;
      }
    }

    if (minSegSize <= 0) {
      minSegSize = OGlobalConfiguration.STORAGE_DOUBLE_WRITE_LOG_MIN_SEG_SIZE.getValueAsLong() * 1024 * 1024;
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
        checkOpen();
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        // THIS OPEN THE STORAGE ONLY THE FIRST TIME
        storage.open(config.getConfigurations());
        storage.incOnOpen();
        embedded = newSessionInstance(storage);
        embedded.init(config, getOrCreateSharedContext(storage));
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(user, "nopwd", false);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(OAbstractPaginatedStorage storage) {
    return new ODatabaseDocumentEmbedded(storage);
  }

  public ODatabaseDocumentEmbedded openNoAuthorization(String name) {
    try {
      final ODatabaseDocumentEmbedded embedded;
      OrientDBConfig config = solveConfig(null);
      synchronized (this) {
        checkOpen();
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        // THIS OPEN THE STORAGE ONLY THE FIRST TIME
        storage.open(config.getConfigurations());
        storage.incOnOpen();
        embedded = newSessionInstance(storage);
        embedded.init(config, getOrCreateSharedContext(storage));
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
        OAbstractPaginatedStorage storage = getOrInitStorage(name);
        // THIS OPEN THE STORAGE ONLY THE FIRST TIME
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

        embedded = newSessionInstance(storage);
        embedded.init(config, getOrCreateSharedContext(storage));
        storage.incOnOpen();
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
      embedded = newPooledSessionInstance(pool, storage);
      embedded.init(pool.getConfig(), getOrCreateSharedContext(storage));
      storage.incOnOpen();
    }
    embedded.rebuildIndexes();
    embedded.internalOpen(user, password);
    embedded.callOnOpenListeners();
    return embedded;
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(ODatabasePoolInternal pool, OAbstractPaginatedStorage storage) {
    return new ODatabaseDocumentEmbeddedPooled(pool, storage);
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
            .createStorage(buildName(name), new HashMap<>(), maxWALSegmentSize, doubleWriteLogMaxSegSize, generateStorageId());
        if (storage.exists()) {
          storages.put(name, storage);
        }
      }
    }
    return storage;
  }

  protected final int generateStorageId() {
    int storageId = Math.abs(nextStorageId.getAndIncrement());
    while (!currentStorageIds.add(storageId)) {
      storageId = Math.abs(nextStorageId.getAndIncrement());
    }

    return storageId;
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
          if (type == ODatabaseType.MEMORY) {
            storage = (OAbstractPaginatedStorage) memory
                .createStorage(name, new HashMap<>(), maxWALSegmentSize, doubleWriteLogMaxSegSize, generateStorageId());
          } else {
            storage = (OAbstractPaginatedStorage) disk
                .createStorage(buildName(name), new HashMap<>(), maxWALSegmentSize, doubleWriteLogMaxSegSize, generateStorageId());
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
    final ODatabaseDocumentEmbedded embedded;
    OAbstractPaginatedStorage storage;
    synchronized (this) {
      if (!exists(name, null, null)) {
        try {
          storage = (OAbstractPaginatedStorage) disk
              .createStorage(buildName(name), new HashMap<>(), maxWALSegmentSize, doubleWriteLogMaxSegSize, generateStorageId());
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
        OSharedContext context = sharedContexts.remove(name);
        if (context != null) {
          context.close();
        }
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
    final ODatabaseDocumentEmbedded embedded = newSessionInstance(storage);
    embedded.setSerializer(serializer);
    embedded.internalCreate(config, getOrCreateSharedContext(storage));
    return embedded;
  }

  protected synchronized OSharedContext getOrCreateSharedContext(OAbstractPaginatedStorage storage) {
    OSharedContext result = sharedContexts.get(storage.getName());
    if (result == null) {
      result = createSharedContext(storage);
      sharedContexts.put(storage.getName(), result);
    }
    return result;
  }

  protected OSharedContext createSharedContext(OAbstractPaginatedStorage storage) {
    return new OSharedContextEmbedded(storage, this);
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
        OSharedContext sharedContext = sharedContexts.get(name);
        if (sharedContext != null) {
          sharedContext.close();
        }
        final int storageId = storage.getId();
        storage.delete();
        storages.remove(name);
        currentStorageIds.remove(storageId);
        sharedContexts.remove(name);
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

  @Override
  public void close() {
    if (!open)
      return;
    timer.cancel();
    executor.shutdown();
    try {
      while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        OLogManager.instance().warn(this, "Failed waiting background operations termination");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    synchronized (this) {
      removeShutdownHook();
      internalClose();
      currentStorageIds.clear();
    }
  }

  public synchronized void internalClose() {
    if (!open)
      return;
    open = false;
    this.sharedContexts.values().forEach(x -> x.close());
    final List<OAbstractPaginatedStorage> storagesCopy = new ArrayList<>(storages.values());

    Exception storageException = null;

    for (OAbstractPaginatedStorage stg : storagesCopy) {
      try {
        OLogManager.instance().info(this, "- shutdown storage: " + stg.getName() + "...");
        stg.shutdown();
      } catch (Exception e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
        storageException = e;
      } catch (Error e) {
        OLogManager.instance().warn(this, "-- error on shutdown storage", e);
        throw e;
      }
    }
    this.sharedContexts.clear();
    storages.clear();
    orient.onEmbeddedFactoryClose(this);
    if (autoCloseTimer != null) {
      autoCloseTimer.cancel();
    }

    if (storageException != null) {
      throw OException.wrapException(new OStorageException("Error during closing the storages"), storageException);
    }
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
          .createStorage(path, new HashMap<>(), maxWALSegmentSize, doubleWriteLogMaxSegSize, generateStorageId());
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
      OSharedContext ctx = sharedContexts.remove(iDatabaseName);
      if (ctx != null) {
        ctx.close();
      }
      storage.shutdown();
    }
  }

  public String getDatabasePath(String iDatabaseName) {
    OAbstractPaginatedStorage storage = storages.get(iDatabaseName);
    if (storage != null && storage instanceof OLocalPaginatedStorage)
      return ((OLocalPaginatedStorage) storage).getStoragePath().toString();
    return null;
  }

  protected void checkOpen() {
    if (!open)
      throw new ODatabaseException("OrientDB Instance is closed");
  }

  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

  public void schedule(TimerTask task, long delay, long period) {
    timer.schedule(task, delay, period);
  }

  public void scheduleOnce(TimerTask task, long delay) {
    timer.schedule(task, delay);
  }

  @Override
  public <X> Future<X> execute(String database, String user, ODatabaseTask<X> task) {
    return executor.submit(() -> {
      try (ODatabaseSession session = openNoAuthenticate(database, user)) {
        return task.call(session);
      }
    });
  }

  @Override
  public <X> Future<X> executeNoAuthorization(String database, ODatabaseTask<X> task) {
    return executor.submit(() -> {
      try (ODatabaseSession session = openNoAuthorization(database)) {
        return task.call(session);
      }
    });
  }

  public <X> Future<X> executeNoDb(Callable<X> callable) {
    return executor.submit(callable);
  }

}
