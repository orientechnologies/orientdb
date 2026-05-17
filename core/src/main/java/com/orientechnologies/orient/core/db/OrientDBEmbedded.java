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

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_DELAY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.FILE_DELETE_RETRY;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.WARNING_DEFAULT_USERS;

import com.orientechnologies.common.concur.lock.OModificationOperationProhibitedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.thread.OSourceTraceExecutorService;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.security.ODefaultSecuritySystem;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultSetReady;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.sql.parser.OServerStatement;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEngine;
import com.orientechnologies.orient.core.storage.OStorageEngine.RegisterResult;
import com.orientechnologies.orient.core.storage.config.OClusterBasedStorageConfiguration;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Created by tglman on 08/04/16. */
public class OrientDBEmbedded implements OrientDBInternal {
  private static final OLogger logger = OLogManager.instance().logger(OrientDBEmbedded.class);

  private static final AtomicLong queryCounter = new AtomicLong(0);
  protected ThreadGroup allGroups;
  protected ThreadGroup threadsGroup;

  protected final Map<String, OStorage> storages = new ConcurrentHashMap<>();
  protected final Map<String, OSharedContext> sharedContexts = new ConcurrentHashMap<>();
  protected final Set<ODatabasePoolInternal> pools =
      Collections.newSetFromMap(new ConcurrentHashMap<>());
  protected final OrientDBConfig configurations;
  protected final String basePath;
  protected final Orient orient;
  protected final OCachedDatabasePoolFactory cachedPoolFactory;
  private volatile boolean open = true;
  private final ExecutorService executor;
  private final ExecutorService ioExecutor;
  private final Timer timer;
  private OCancellableTimer autoCloseTimer = null;
  private final OScriptManager scriptManager;
  private final OSystemDatabase systemDatabase;
  private final ODefaultSecuritySystem securitySystem;
  private final OCommandTimeoutChecker timeoutChecker;
  private final Map<String, OStorageEngine> engines = new HashMap<>();
  protected final AtomicInteger dbCount = new AtomicInteger(0);

  public OrientDBEmbedded(String directoryPath, OrientDBConfig configurations, Orient orient) {
    super();
    this.orient = orient;
    orient.onEmbeddedFactoryInit(this);
    directoryPath = directoryPath.trim();
    if (directoryPath.length() != 0) {
      final File dirFile = new File(directoryPath);
      if (!dirFile.exists()) {
        logger.infoNoDb("Directory %s does not exist, try to create it.", dirFile);

        if (!dirFile.mkdirs()) {
          logger.errorNoDb("Can not create directory %s", null, dirFile);
        }
      }
      this.basePath = dirFile.getAbsolutePath();
    } else {
      this.basePath = null;
    }
    allGroups = new ThreadGroup("OrientDB Context");
    threadsGroup = new ThreadGroup(allGroups, "Threads");

    this.configurations = configurations != null ? configurations : OrientDBConfig.defaultConfig();
    initEngines();

    orient.addOrientDB(this);
    executor = newExecutor();
    ioExecutor = newIoExecutor();
    String timerName;
    if (basePath != null) {
      timerName = "embedded:" + basePath;
    } else {
      timerName = "memory:";
    }
    timer = new Timer("OrientDB Timer[" + timerName + "]");

    cachedPoolFactory = createCachedDatabasePoolFactory(this.configurations);

    initAutoClose();

    long timeout = getLongConfig(OGlobalConfiguration.COMMAND_TIMEOUT);
    timeoutChecker = new OCommandTimeoutChecker(timeout, this);
    systemDatabase = new OSystemDatabase(this);
    securitySystem = new ODefaultSecuritySystem();
    securitySystem.activate(this, this.configurations.getSecurityConfig());
    this.scriptManager = new OScriptManager(this);
  }

  protected void initEngines() {
    Iterator<OStorageEngine> engines =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OStorageEngine.class);

    while (engines.hasNext()) {
      OStorageEngine engine = null;
      try {
        engine = engines.next();
        Path path = null;
        if (basePath != null) {
          path = Path.of(basePath);
        }
        OStorageEngine prev = this.engines.get(engine.getName());
        if (prev != null) {
          if (prev.getClass().isAssignableFrom(engine.getClass())) {
            engine.init(path, this.configurations.getConfigurations());
            this.engines.put(engine.getName(), engine);
          } else {
            throw new IllegalArgumentException("Cannot replace storage engine " + engine.getName());
          }
        } else {
          engine.init(path, this.configurations.getConfigurations());
          this.engines.put(engine.getName(), engine);
        }
      } catch (IllegalArgumentException e) {
        if (engine != null) logger.debug("Failed to replace engine %s", e, engine.getName());
      }
    }
  }

  public OStorageEngine getEngine(String engine) {
    return this.engines.get(engine);
  }

  public OStorageEngine getDefaultEngine() {
    return getEngine("plocal");
  }

  private void initAutoClose() {

    boolean autoClose = getBoolConfig(OGlobalConfiguration.AUTO_CLOSE_AFTER_DELAY);
    if (autoClose) {
      int autoCloseDelay = getIntConfig(OGlobalConfiguration.AUTO_CLOSE_DELAY);
      final long delay = autoCloseDelay * 60 * 1000;
      initAutoClose(delay);
    }
  }

  private ExecutorService newIoExecutor() {
    if (getBoolConfig(OGlobalConfiguration.EXECUTOR_POOL_IO_ENABLED)) {
      int ioSize = excutorMaxSize(OGlobalConfiguration.EXECUTOR_POOL_IO_MAX_SIZE);
      ExecutorService exec =
          OThreadPoolExecutors.newScalingThreadPool(
              "IO", allGroups, excutorBaseSize(ioSize), ioSize, ioSize, 30, TimeUnit.MINUTES);
      if (getBoolConfig(OGlobalConfiguration.EXECUTOR_DEBUG_TRACE_SOURCE)) {
        exec = new OSourceTraceExecutorService(exec);
      }
      return exec;
    } else {
      return null;
    }
  }

  protected void runOnThread(Runnable run) {
    Thread thread = new Thread(threadsGroup, run);
    thread.setDaemon(true);
    thread.start();
  }

  private ExecutorService newExecutor() {
    int size = excutorMaxSize(OGlobalConfiguration.EXECUTOR_POOL_MAX_SIZE);
    ExecutorService exec =
        OThreadPoolExecutors.newScalingThreadPool(
            "Executor", allGroups, excutorBaseSize(size), size, size, 30, TimeUnit.MINUTES);
    if (getBoolConfig(OGlobalConfiguration.EXECUTOR_DEBUG_TRACE_SOURCE)) {
      exec = new OSourceTraceExecutorService(exec);
    }
    return exec;
  }

  protected boolean getBoolConfig(OGlobalConfiguration config) {
    return this.configurations.getConfigurations().getValueAsBoolean(config);
  }

  protected int getIntConfig(OGlobalConfiguration config) {
    return this.configurations.getConfigurations().getValueAsInteger(config);
  }

  protected long getLongConfig(OGlobalConfiguration config) {
    return this.configurations.getConfigurations().getValueAsLong(config);
  }

  protected int excutorMaxSize(OGlobalConfiguration config) {
    int size = getIntConfig(config);
    if (size == 0) {
      logger.warn(
          "Configuration %s has a value 0 using number of CPUs as base value", config.getKey());
      size = Runtime.getRuntime().availableProcessors();
    } else if (size <= -1) {
      size = Runtime.getRuntime().availableProcessors();
    }
    if (size < 2) {
      size = 2;
    }
    return size;
  }

  protected int excutorBaseSize(int size) {
    int baseSize;

    if (size > 10) {
      baseSize = size / 10;
    } else if (size > 4) {
      baseSize = size / 2;
    } else {
      baseSize = size;
    }
    return baseSize;
  }

  protected OCachedDatabasePoolFactory createCachedDatabasePoolFactory(OrientDBConfig config) {
    int capacity = getIntConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY);
    long timeout = getIntConfig(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT);
    return new OCachedDatabasePoolFactoryImpl(this, capacity, timeout);
  }

  public void initAutoClose(long delay) {
    final long scheduleTime = delay / 3;
    autoCloseTimer = periodicExecute(() -> checkAndCloseStorages(delay), scheduleTime);
  }

  private synchronized void checkAndCloseStorages(long delay) {
    Set<String> toClose = new HashSet<>();
    for (OSharedContext contex : sharedContexts.values()) {
      if (contex.getStorage().getType().equalsIgnoreCase(ODatabaseType.PLOCAL.name())
          && contex.getSessionCount() == 0) {
        long currentTime = System.currentTimeMillis();
        if (currentTime > contex.getLastCloseTime() + delay) {
          toClose.add(contex.getStorage().getName());
        }
      }
    }
    for (String storage : toClose) {
      forceDatabaseClose(storage);
    }
  }

  @Override
  public ODatabaseDocumentInternal open(String name, String user, String password) {
    return open(name, user, password, null);
  }

  public ODatabaseDocumentEmbedded openNoAuthenticate(String name, String user) {
    checkDatabaseName(name);
    try {
      final ODatabaseDocumentEmbedded embedded;
      OrientDBConfig config = solveConfig(null);
      synchronized (this) {
        checkOpen();
        OStorage storage = getAndOpenStorage(name, config);
        embedded = newSessionInstance(storage, config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(user, "nopwd", false);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  protected ODatabaseDocumentEmbedded newSessionInstance(OStorage storage, OrientDBConfig config) {
    OSharedContext sharedContext = getOrCreateSharedContext(storage);
    ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage, sharedContext);
    embedded.init(config);
    return embedded;
  }

  protected ODatabaseDocumentEmbedded newCreateSessionInstance(
      OStorage storage, OrientDBConfig config) {
    OSharedContext sharedContext = getOrCreateSharedContext(storage);
    ODatabaseDocumentEmbedded embedded = new ODatabaseDocumentEmbedded(storage, sharedContext);
    embedded.internalCreate(config, sharedContext);
    return embedded;
  }

  protected ODatabaseDocumentEmbedded onlyOpenNoAuthorization(String name) {
    checkDatabaseName(name);
    try {
      final ODatabaseDocumentEmbedded embedded;
      synchronized (this) {
        checkOpen();
        OStorage storage = storages.get(name);
        OSharedContext sharedContext = sharedContexts.get(name);
        if (storage != null && sharedContext != null) {
          embedded = new ODatabaseDocumentEmbedded(storage, sharedContext);
          OrientDBConfig config = solveConfig(null);
          embedded.init(config);
          return embedded;
        } else {
          return null;
        }
      }
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  public ODatabaseDocumentEmbedded openNoAuthorization(String name) {
    checkDatabaseName(name);
    try {
      final ODatabaseDocumentEmbedded embedded;
      OrientDBConfig config = solveConfig(null);
      synchronized (this) {
        checkOpen();
        OStorage storage = getAndOpenStorage(name, config);
        embedded = newSessionInstance(storage, config);
      }
      embedded.rebuildIndexes();
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  public ODatabaseDocumentInternal open(
      String name, String user, String password, OrientDBConfig config) {
    checkDatabaseName(name);
    checkDefaultPassword(name, user, password);
    try {
      final ODatabaseDocumentEmbedded embedded;
      synchronized (this) {
        checkOpen();
        config = solveConfig(config);
        OStorage storage = getAndOpenStorage(name, config);

        embedded = newSessionInstance(storage, config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(user, password);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot open database '" + name + "'"), e);
    }
  }

  @Override
  public ODatabaseDocumentInternal open(
      OAuthenticationInfo authenticationInfo, OrientDBConfig config) {
    try {
      final ODatabaseDocumentEmbedded embedded;
      synchronized (this) {
        checkOpen();
        config = solveConfig(config);
        if (!authenticationInfo.getDatabase().isPresent()) {
          throw new OSecurityException("Authentication info do not contain the database");
        }
        String database = authenticationInfo.getDatabase().get();
        OStorage storage = getAndOpenStorage(database, config);
        embedded = newSessionInstance(storage, config);
      }
      embedded.rebuildIndexes();
      embedded.internalOpen(authenticationInfo);
      embedded.callOnOpenListeners();
      return embedded;
    } catch (Exception e) {
      throw OException.wrapException(
          new ODatabaseException("Cannot open database '" + authenticationInfo.getDatabase() + "'"),
          e);
    }
  }

  protected OStorage getAndOpenStorage(String name, OrientDBConfig config) {
    OStorage storage = storages.get(name);
    if (storage == null) {
      storage = getDefaultEngine().openLocal(this, name, config.getConfigurations());
      if (storage.exists()) {
        storages.put(name, storage);
      }
    }
    return storage;
  }

  private void checkDefaultPassword(String database, String user, String password) {
    if ((("admin".equals(user) && "admin".equals(password))
            || ("reader".equals(user) && "reader".equals(password))
            || ("writer".equals(user) && "writer".equals(password))
            || ("root".equals(user) && "root".equals(password))
            || ("orientdb".equals(user) && "orientdb".equals(password)))
        && WARNING_DEFAULT_USERS.getValueAsBoolean()) {
      logger.warnNoDb(
          String.format(
              "IMPORTANT! Using default password is unsafe, please change password for user"
                  + " '%s' on database '%s'",
              user, database));
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

  public ODatabaseDocumentInternal poolOpen(
      String name, String user, String password, ODatabasePoolInternal pool) {
    final ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      checkOpen();
      OStorage storage = getAndOpenStorage(name, pool.getConfig());
      embedded = newPooledSessionInstance(pool, storage, getOrCreateSharedContext(storage));
    }
    embedded.rebuildIndexes();
    embedded.internalOpen(user, password);
    embedded.callOnOpenListeners();
    return embedded;
  }

  protected ODatabaseDocumentEmbedded newPooledSessionInstance(
      ODatabasePoolInternal pool, OStorage storage, OSharedContext sharedContext) {
    ODatabaseDocumentEmbeddedPooled embedded =
        new ODatabaseDocumentEmbeddedPooled(pool, storage, sharedContext);
    embedded.init(pool.getConfig());
    return embedded;
  }

  public synchronized OStorage getStorage(String name) {
    return storages.get(name);
  }

  protected String buildName(String name) {
    if (basePath == null) {
      throw new ODatabaseException(
          "OrientDB instanced created without physical path, only memory databases are allowed");
    }
    return basePath + "/" + name;
  }

  public void create(String name, String user, String password, ODatabaseType type) {
    create(name, user, password, type, null);
  }

  @Override
  public void create(
      String name, String user, String password, ODatabaseType type, OrientDBConfig config) {
    create(name, user, password, type, new ODatabaseId(), config, null);
  }

  @Override
  public void create(
      String name,
      String user,
      String password,
      ODatabaseType type,
      ODatabaseId id,
      OrientDBConfig config,
      ODatabaseTask<Void> createOps) {
    checkDatabaseName(name);
    final ODatabaseDocumentEmbedded embedded;
    synchronized (this) {
      if (!exists(name, user, password)) {
        try {
          config = solveConfig(config);
          OStorage storage;
          if (type == ODatabaseType.MEMORY) {
            storage = getDefaultEngine().createMemory(this, id, name, config.getConfigurations());
          } else {
            storage = getDefaultEngine().createLocal(this, id, name, config.getConfigurations());
          }
          storages.put(name, storage);
          embedded = internalCreate(config, storage);
          if (createOps != null) {
            OScenarioThreadLocal.executeAsDistributed(
                () -> {
                  createOps.call(embedded);
                  return null;
                });
          }
        } catch (Exception e) {
          throw OException.wrapException(
              new ODatabaseException("Cannot create database '" + name + "'"), e);
        }
      } else {
        throw new ODatabaseException(
            "Cannot create new database '" + name + "' because it already exists");
      }
    }
    embedded.callOnCreateListeners();
    ODatabaseRecordThreadLocal.instance().remove();
  }

  protected void distributedSetOnline(OStorage database) {}

  @Override
  public boolean networkRestore(String name, ODatabaseId databaseId, InputStream in) {
    checkDatabaseName(name);
    OStorage storage = null;
    OContextConfiguration config = getConfigurations().getConfigurations();
    try {
      OSharedContext context;
      synchronized (this) {
        context = sharedContexts.get(name);
        if (context != null) {
          context.close();
        }
        storage = storages.get(name);
        if (storage == null) {
          storage = getDefaultEngine().createForRestoreLocal(this, databaseId, name, config);
          storages.put(name, storage);
        }
      }
      storage.restoreNetwork(in);
      dbCount.incrementAndGet();
      distributedSetOnline(storage);
      return true;
    } catch (OModificationOperationProhibitedException e) {
      throw e;
    } catch (Exception e) {
      logger.warn("failed non sync of database %s", e, name);
      synchronized (this) {
        dbCount.decrementAndGet();
        storages.remove(name);
      }
      return false;
    }
  }

  public void restore(
      String name,
      String user,
      String password,
      ODatabaseType type,
      String path,
      OrientDBConfig config) {
    checkDatabaseName(name);
    config = solveConfig(config);
    final ODatabaseDocumentEmbedded embedded;
    OStorage storage;
    synchronized (this) {
      if (!exists(name, null, null)) {
        try {
          storage =
              getDefaultEngine()
                  .createLocal(this, new ODatabaseId(), name, config.getConfigurations());
          embedded = internalCreate(config, storage);
          storages.put(name, storage);
        } catch (Exception e) {
          throw OException.wrapException(
              new ODatabaseException("Cannot restore database '" + name + "'"), e);
        }
      } else {
        throw new ODatabaseException(
            "Cannot create new storage '" + name + "' because it already exists");
      }
    }
    storage.restoreFromIncrementalBackup(path);
    embedded.callOnCreateListeners();
    embedded.getSharedContext().reInit(storage, embedded);
    ODatabaseRecordThreadLocal.instance().remove();
  }

  public void restore(
      String name,
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      OCommandOutputListener iListener) {
    checkDatabaseName(name);
    synchronized (this) {
      dbCount.decrementAndGet();
      OSharedContext context = sharedContexts.remove(name);
      if (context != null) {
        context.close();
      }
      OStorage storage = storages.get(name);
      if (storage != null) {
        storage.close();
      }
    }
    OContextConfiguration config = getConfigurations().getConfigurations();
    OStorage storage;
    synchronized (this) {
      storage =
          getDefaultEngine().createForRestoreLocal(this, new ODatabaseId("mock"), name, config);
      storages.put(name, storage);
      dbCount.incrementAndGet();
    }
    try {
      storage.restore(in, options, iListener);
    } catch (Exception e) {
      synchronized (this) {
        dbCount.decrementAndGet();
        storage.delete();
        OLocalPaginatedStorage.deleteFilesFromDisc(
            name,
            config.getValueAsInteger(FILE_DELETE_RETRY),
            config.getValueAsInteger(FILE_DELETE_DELAY),
            name);
        storages.remove(name);
      }
      throw OException.wrapException(
          new ODatabaseException("Cannot create database '" + name + "'"), e);
    }
  }

  protected ODatabaseDocumentEmbedded internalCreate(OrientDBConfig config, OStorage storage) {
    return newCreateSessionInstance(storage, config);
  }

  protected synchronized OSharedContext getOrCreateSharedContext(OStorage storage) {
    OSharedContext result = sharedContexts.get(storage.getName());
    if (result == null) {
      result = createSharedContext(storage);
      dbCount.incrementAndGet();
      sharedContexts.put(storage.getName(), result);
    }
    return result;
  }

  protected OSharedContext createSharedContext(OStorage storage) {
    return new OSharedContextEmbedded(storage, this);
  }

  @Override
  public synchronized boolean exists(String name, String user, String password) {
    checkOpen();
    OStorage storage = storages.get(name);
    if (storage == null) {
      if (basePath != null) {
        return getDefaultEngine().exists(name);
      } else {
        return false;
      }
    }
    return storage.exists();
  }

  @Override
  public void internalDrop(String database) {
    this.drop(database, null, null);
  }

  @Override
  public void drop(String name, String user, String password) {
    synchronized (this) {
      checkOpen();
    }
    checkDatabaseName(name);
    ODatabaseDocumentInternal current = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      ODatabaseDocumentInternal db = openNoAuthenticate(name, user);
      for (Iterator<ODatabaseLifecycleListener> it = orient.getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onDrop(db);
      }
      db.callOnDropListeners();
      db.close();
    } catch (OStorageException e) {
      logger.warnNoDb("Error opening %s for drop hook call ", name, e);
    } finally {
      ODatabaseRecordThreadLocal.instance().set(current);
      synchronized (this) {
        if (exists(name, user, password)) {
          OStorage storage = getAndOpenStorage(name, getConfigurations());
          OSharedContext sharedContext = sharedContexts.get(name);
          if (sharedContext != null) {
            sharedContext.close();
          }
          storage.delete();
          storages.remove(name);
          dbCount.decrementAndGet();
          sharedContexts.remove(name);
        }
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
    // TODO: Verify validity this generic permission on guest
    if (!securitySystem.isAuthorized("guest", "server.listDatabases.system")) {
      databases.remove(OSystemDatabase.SYSTEM_DB_NAME);
    }
    return databases;
  }

  public synchronized void loadAllDatabases() {
    if (basePath != null) {
      scanDatabaseDirectory(
          new File(basePath),
          (name) -> {
            if (!storages.containsKey(name)) {
              getAndOpenStorage(name, getConfigurations());
            }
          });
    }
  }

  public ODatabasePoolInternal openPool(String name, String user, String password) {
    return openPool(name, user, password, null);
  }

  @Override
  public ODatabasePoolInternal openPool(
      String name, String user, String password, OrientDBConfig config) {
    checkDatabaseName(name);
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
    checkDatabaseName(database);
    checkOpen();
    ODatabasePoolInternal pool =
        cachedPoolFactory.get(database, user, password, solveConfig(config));
    pools.add(pool);
    return pool;
  }

  @Override
  public void close() {
    if (!open) {
      return;
    }
    timeoutChecker.close();
    timer.cancel();
    securitySystem.shutdown();
    executor.shutdown();
    preClose();
    try {
      while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        logger.warn("Failed waiting background operations termination");
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    threadsGroup.interrupt();
    synchronized (this) {
      scriptManager.closeAll();
      internalClose();
    }
    for (OStorageEngine engine : this.engines.values()) {
      engine.shutdown();
    }
    if (ioExecutor != null) {
      try {
        ioExecutor.shutdown();
        while (!ioExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
          logger.warn("Failed waiting background io operations termination");
          ioExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    allGroups.interrupt();
    removeShutdownHook();
  }

  public synchronized void preClose() {
    if (!open) {
      return;
    }
    this.sharedContexts.values().forEach(x -> x.getViewManager().close());
  }

  public synchronized void internalClose() {
    if (!open) {
      return;
    }
    open = false;
    this.sharedContexts.values().forEach(x -> x.close());
    final List<OStorage> storagesCopy = new ArrayList<>(storages.values());

    Exception storageException = null;

    for (OStorage stg : storagesCopy) {
      try {
        logger.info("- shutdown storage: %s ...", stg.getName());
        stg.shutdown();
      } catch (Exception e) {
        logger.warn("-- error on shutdown storage", e);
        storageException = e;
      } catch (Error e) {
        logger.warn("-- error on shutdown storage", e);
        throw e;
      }
    }
    dbCount.set(0);
    this.sharedContexts.clear();
    storages.clear();
    orient.onEmbeddedFactoryClose(this);
    if (autoCloseTimer != null) {
      autoCloseTimer.cancel();
    }

    if (storageException != null) {
      throw OException.wrapException(
          new OStorageException("Error during closing the storages"), storageException);
    }
  }

  public OrientDBConfig getConfigurations() {
    return configurations;
  }

  public void removePool(ODatabasePoolInternal pool) {
    pools.remove(pool);
  }

  private static void scanDatabaseDirectory(final File directory, DatabaseFound found) {
    if (directory.exists() && directory.isDirectory()) {
      final File[] files = directory.listFiles();
      if (files != null) {
        for (File db : files) {
          if (db.isDirectory()) {
            for (File cf : db.listFiles()) {
              String fileName = cf.getName();
              if (fileName.equals("database.ocf")
                  || (fileName.startsWith(OClusterBasedStorageConfiguration.COMPONENT_NAME)
                      && fileName.endsWith(
                          OClusterBasedStorageConfiguration.DATA_FILE_EXTENSION))) {
                found.found(db.getName());
                break;
              }
            }
          }
        }
      }
    }
  }

  public synchronized void initCustomStorage(
      String name, String path, String userName, String userPassword) {
    synchronized (this) {
      Path p = Paths.get(path);
      RegisterResult registerd =
          getDefaultEngine().registerLocal(this, name, p, getConfigurations().getConfigurations());
      // TODO: Add Creation settings and parameters
      storages.put(name, registerd.storage());
      if (registerd.created()) {
        newCreateSessionInstance(registerd.storage(), configurations);
      } else {
        newSessionInstance(registerd.storage(), configurations).close();
      }
    }
  }

  public void removeShutdownHook() {
    orient.removeOrientDB(this);
  }

  public synchronized Collection<OStorage> getStorages() {
    return storages.values();
  }

  public synchronized void forceDatabaseClose(String iDatabaseName) {
    OStorage storage = storages.remove(iDatabaseName);
    if (storage != null) {
      OSharedContext ctx = sharedContexts.remove(iDatabaseName);
      dbCount.decrementAndGet();
      if (ctx != null) {
        ctx.getViewManager().close();
        ctx.close();
      }
      storage.shutdown();
    }
  }

  public String getDatabasePath(String iDatabaseName) {
    OStorage storage = storages.get(iDatabaseName);
    if (storage != null) {
      Optional<Path> path = storage.getPath();
      if (path.isPresent()) {
        return path.get().toString();
      }
    }
    return null;
  }

  protected void checkOpen() {
    if (!open) {
      throw new ODatabaseException("OrientDB Instance is closed");
    }
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
  public OCancellableTimer delayExecute(Runnable toExecuted, long delayMills) {
    TimerTask tt =
        new TimerTask() {
          @Override
          public void run() {
            execute(toExecuted);
          }
        };
    timer.schedule(tt, delayMills);
    return new OCancellableTimerTask(tt);
  }

  @Override
  public OCancellableTimer periodicExecute(Runnable toExecuted, long periodicMills) {
    TimerTask tt =
        new TimerTask() {
          @Override
          public void run() {
            execute(toExecuted);
          }
        };
    timer.schedule(tt, periodicMills, periodicMills);
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
  public <X> Future<X> execute(String database, String user, ODatabaseTask<X> task) {
    return executor.submit(
        () -> {
          try (ODatabaseSession session = openNoAuthenticate(database, user)) {
            return task.call(session);
          }
        });
  }

  @Override
  public Future<?> execute(Runnable task) {
    return executor.submit(task);
  }

  @Override
  public <X> Future<X> execute(Callable<X> task) {
    return executor.submit(task);
  }

  @Override
  public <X> Future<X> executeNoAuthorizationOnActive(String database, ODatabaseTask<X> task) {
    return executor.submit(
        () -> {
          if (isOpen()) {
            ODatabaseSession sess = onlyOpenNoAuthorization(database);
            if (sess != null) {
              try (ODatabaseSession session = sess) {
                return task.call(session);
              }
            } else {
              return null;
            }
          } else {
            logger.warn(" Cancelled execution of task, OrientDB instance is closed");
            return null;
          }
        });
  }

  @Override
  public <X> Future<X> executeNoAuthorization(String database, ODatabaseTask<X> task) {
    return executor.submit(
        () -> {
          if (isOpen()) {
            try (ODatabaseSession session = openNoAuthorization(database)) {
              return task.call(session);
            }
          } else {
            logger.warn(" Cancelled execution of task, OrientDB instance is closed");
            return null;
          }
        });
  }

  public <X> Future<X> executeNoDb(Callable<X> callable) {
    return executor.submit(callable);
  }

  public OScriptManager getScriptManager() {
    return scriptManager;
  }

  private String newQueryId() {
    return "" + System.currentTimeMillis() + "_" + queryCounter.incrementAndGet();
  }

  public OResultSet executeServerStatement(
      String script, String username, String pw, Map<String, Object> args) {
    OServerStatement statement = OSQLEngine.parseServerStatement(script, this);
    OResultSet original = statement.execute(this, args, true);
    OLocalResultSetLifecycleDecorator result;

    OResultSetReady prefetched = new OResultSetReady();
    original.forEachRemaining(x -> prefetched.add(x));
    original.close();
    result = new OLocalResultSetLifecycleDecorator(prefetched, newQueryId());

    return result;
  }

  public OResultSet executeServerStatement(
      String script, String username, String pw, Object... args) {
    OServerStatement statement = OSQLEngine.parseServerStatement(script, this);
    OResultSet original = statement.execute(this, args, true);
    OLocalResultSetLifecycleDecorator result;

    OResultSetReady prefetched = new OResultSetReady();
    original.forEachRemaining(x -> prefetched.add(x));
    original.close();
    result = new OLocalResultSetLifecycleDecorator(prefetched, newQueryId());

    return result;
  }

  @Override
  public OSystemDatabase getSystemDatabase() {
    return systemDatabase;
  }

  public ODefaultSecuritySystem getSecuritySystem() {
    return securitySystem;
  }

  @Override
  public String getBasePath() {
    return basePath;
  }

  public boolean isMemoryOnly() {
    return basePath == null;
  }

  protected void checkDatabaseName(String name) {
    Objects.requireNonNull(name, "Database name is null");
    if (name.contains("/") || name.contains(":")) {
      throw new ODatabaseException(String.format("Invalid database name:'%s'", name));
    }
  }

  public Set<String> listLodadedDatabases() {
    Set<String> dbs;
    synchronized (this) {
      dbs = new HashSet<String>(storages.keySet());
    }
    dbs.remove(OSystemDatabase.SYSTEM_DB_NAME);
    return dbs;
  }

  public void startCommand(Optional<Long> timeout) {
    timeoutChecker.startCommand(timeout);
  }

  public void endCommand() {
    timeoutChecker.endCommand();
  }

  @Override
  public String getConnectionUrl() {
    String connectionUrl = "embedded:";
    if (basePath != null) {
      connectionUrl += basePath;
    }
    return connectionUrl;
  }

  public ExecutorService getIoExecutor() {
    return ioExecutor;
  }

  @Override
  public ONodeId getNodeId() {
    return new ONodeId("$$unnamed");
  }
}
