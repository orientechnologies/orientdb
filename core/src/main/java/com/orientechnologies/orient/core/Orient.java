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
package com.orientechnologies.orient.core;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.core.cache.OLocalRecordCacheFactory;
import com.orientechnologies.orient.core.cache.OLocalRecordCacheFactoryImpl;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategyFactory;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseThreadLocalFactory;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.ORecordFactoryManager;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.shutdown.OShutdownHandler;
import com.orientechnologies.orient.core.storage.OIdentifiableStorage;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.File;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Orient extends OListenerManger<OOrientListener> {
  public static final String                                                         ORIENTDB_HOME                 = "ORIENTDB_HOME";
  public static final String                                                         URL_SYNTAX                    = "<engine>:<db-type>:<db-name>[?<db-param>=<db-value>[&]]*";

  private static final Orient                                                        instance                      = new Orient();
  private static volatile boolean                                                    registerDatabaseByPath        = false;

  private final ConcurrentMap<String, OEngine>                                       engines                       = new ConcurrentHashMap<String, OEngine>();
  private final ConcurrentMap<String, OStorage>                                      storages                      = new ConcurrentHashMap<String, OStorage>();
  private final ConcurrentHashMap<Integer, Boolean>                                  storageIds                    = new ConcurrentHashMap<Integer, Boolean>();

  private final Map<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY> dbLifecycleListeners          = new LinkedHashMap<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY>();
  private final OScriptManager                                                       scriptManager                 = new OScriptManager();
  private final ThreadGroup                                                          threadGroup;
  private final ReadWriteLock                                                        engineLock                    = new ReentrantReadWriteLock();
  private final ORecordConflictStrategyFactory                                       recordConflictStrategy        = new ORecordConflictStrategyFactory();
  private final ReferenceQueue<OOrientStartupListener>                               removedStartupListenersQueue  = new ReferenceQueue<OOrientStartupListener>();
  private final ReferenceQueue<OOrientShutdownListener>                              removedShutdownListenersQueue = new ReferenceQueue<OOrientShutdownListener>();
  private final Set<OOrientStartupListener>                                          startupListeners              = Collections
      .newSetFromMap(new ConcurrentHashMap<OOrientStartupListener, Boolean>());
  private final Set<WeakHashSetValueHolder<OOrientStartupListener>>                  weakStartupListeners          = Collections
      .newSetFromMap(new ConcurrentHashMap<WeakHashSetValueHolder<OOrientStartupListener>, Boolean>());
  private final Set<WeakHashSetValueHolder<OOrientShutdownListener>>                 weakShutdownListeners         = Collections
      .newSetFromMap(new ConcurrentHashMap<WeakHashSetValueHolder<OOrientShutdownListener>, Boolean>());

  private final PriorityQueue<OShutdownHandler>                                      shutdownHandlers              = new PriorityQueue<OShutdownHandler>(
      11, new Comparator<OShutdownHandler>() {
        @Override
        public int compare(OShutdownHandler handlerOne, OShutdownHandler handlerTwo) {
          if (handlerOne.getPriority() > handlerTwo.getPriority())
            return 1;

          if (handlerOne.getPriority() < handlerTwo.getPriority())
            return -1;

          return 0;
        }
      });

  private final OLocalRecordCacheFactory                                             localRecordCache              = new OLocalRecordCacheFactoryImpl();

  static {
    instance.startup();
  }

  private final String os;

  private volatile Timer                 timer;
  private volatile ORecordFactoryManager recordFactoryManager = new ORecordFactoryManager();
  private OrientShutdownHook             shutdownHook;
  private volatile OProfiler             profiler;
  private ODatabaseThreadLocalFactory    databaseThreadFactory;
  private volatile boolean               active               = false;
  private ThreadPoolExecutor             workers;
  private OSignalHandler                 signalHandler;
  private volatile OSecuritySystem       security;
  private boolean                        runningDistributed   = false;

  private static class WeakHashSetValueHolder<T> extends WeakReference<T> {
    private final int hashCode;

    private WeakHashSetValueHolder(T referent, ReferenceQueue<? super T> q) {
      super(referent, q);
      this.hashCode = referent.hashCode();
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;

      if (o == null || getClass() != o.getClass())
        return false;

      WeakHashSetValueHolder that = (WeakHashSetValueHolder) o;

      if (hashCode != that.hashCode)
        return false;

      final T thisObject = get();
      final Object thatObject = that.get();

      if (thisObject == null && thatObject == null)
        return super.equals(that);
      else if (thisObject != null && thatObject != null)
        return thisObject.equals(thatObject);

      return false;
    }
  }

  protected Orient() {
    super(true);
    this.os = System.getProperty("os.name").toLowerCase();
    threadGroup = new ThreadGroup("OrientDB");
    threadGroup.setDaemon(false);
  }

  public static Orient instance() {
    return instance;
  }

  public static String getHomePath() {
    String v = System.getProperty("orient.home");

    if (v == null)
      v = OSystemVariableResolver.resolveVariable(ORIENTDB_HOME);

    return OFileUtils.getPath(v);
  }

  public static String getTempPath() {
    return OFileUtils.getPath(System.getProperty("java.io.tmpdir") + "/orientdb/");
  }

  /**
   * Tells if to register database by path. Default is false. Setting to true allows to have multiple databases in different path
   * with the same name.
   *
   * @see #setRegisterDatabaseByPath(boolean)
   */
  public static boolean isRegisterDatabaseByPath() {
    return registerDatabaseByPath;
  }

  /**
   * Register database by path. Default is false. Setting to true allows to have multiple databases in different path with the same
   * name.
   */
  public static void setRegisterDatabaseByPath(final boolean iValue) {
    registerDatabaseByPath = iValue;
  }

  public ORecordConflictStrategyFactory getRecordConflictStrategy() {
    return recordConflictStrategy;
  }

  public Orient startup() {
    engineLock.writeLock().lock();
    try {
      if (active)
        // ALREADY ACTIVE
        return this;

      if (timer == null)
        timer = new Timer(true);

      profiler = new OProfilerStub();

      shutdownHook = new OrientShutdownHook();
      if (signalHandler == null) {
        signalHandler = new OSignalHandler();
        signalHandler.installDefaultSignals();
      }

      final int cores = Runtime.getRuntime().availableProcessors();

      workers = new ThreadPoolExecutor(cores, cores * 3, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(cores * 500) {
        @Override
        public boolean offer(Runnable e) {
          // turn offer() and add() into a blocking calls (unless interrupted)
          try {
            put(e);
            return true;
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
          return false;
        }
      });

      registerEngines();

      if (OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP.getValueAsBoolean())
        OGlobalConfiguration.dumpConfiguration(System.out);

      active = true;

      for (OOrientStartupListener l : startupListeners)
        try {
          if (l != null)
            l.onStartup();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on startup", e);
        }

      purgeWeakStartupListeners();
      for (final WeakHashSetValueHolder<OOrientStartupListener> wl : weakStartupListeners)
        try {
          if (wl != null) {
            final OOrientStartupListener l = wl.get();
            if (l != null)
              l.onStartup();
          }

        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on startup", e);
        }

      initShutdownQueue();
    } finally {
      engineLock.writeLock().unlock();
    }

    return this;
  }

  /**
   * Add handler which will be executed during {@link #shutdown()} call.
   *
   * @param shutdownHandler
   *          Shutdown handler instance.
   */
  public void addShutdownHandler(OShutdownHandler shutdownHandler) {
    engineLock.writeLock().lock();
    try {
      shutdownHandlers.add(shutdownHandler);
    } finally {
      engineLock.writeLock().unlock();
    }
  }

  /**
   * Adds shutdown handlers in order which will be used during execution of shutdown.
   */
  private void initShutdownQueue() {
    addShutdownHandler(new OShutdownWorkersHandler());
    addShutdownHandler(new OShutdownEnginesHandler());
    addShutdownHandler(new OShutdownPendingThreadsHandler());
    addShutdownHandler(new OShutdownProfilerHandler());
    addShutdownHandler(new OShutdownCallListenersHandler());
  }

  /**
   * Shutdown whole OrientDB ecosystem. Usually is called during JVM shutdown by JVM shutdown handler. During shutdown all handlers
   * which were registered by the call of {@link #addShutdownHandler(OShutdownHandler)} are called together with pre-registered
   * system shoutdown handlers according to their priority.
   *
   * @see OShutdownWorkersHandler
   * @see
   */
  private void registerEngines() {
    ClassLoader classLoader = Orient.class.getClassLoader();

    Iterator<OEngine> engines = OClassLoaderHelper.lookupProviderWithOrientClassLoader(OEngine.class, classLoader);

    OEngine engine = null;
    while (engines.hasNext()) {
      try {
        engine = engines.next();
        registerEngine(engine);
      } catch (IllegalArgumentException e) {
        if (engine != null)
          OLogManager.instance().debug(this, "Failed to replace engine " + engine.getName());
      }
    }
  }

  public Orient shutdown() {
    engineLock.writeLock().lock();
    try {
      if (!active)
        return this;

      active = false;

      OLogManager.instance().info(this, "Orient Engine is shutting down...");
      for (OShutdownHandler handler : shutdownHandlers) {
        try {
          OLogManager.instance().debug(this, "Shutdown handler %s is going to be called", handler);
          handler.shutdown();
          OLogManager.instance().debug(this, "Shutdown handler %s completed", handler);
        } catch (Exception e) {
          OLogManager.instance().error(this, "Exception during calling of shutdown handler %s", handler);
        }
      }

      shutdownHandlers.clear();
      OLogManager.instance().info(this, "OrientDB Engine shutdown complete");
      OLogManager.instance().flush();
    } finally {
      try {
        removeShutdownHook();
      } finally {
        try {
          removeSignalHandler();
        } finally {
          engineLock.writeLock().unlock();
        }
      }
    }

    return this;
  }

  public void scheduleTask(final TimerTask task, final long delay, final long period) {
    engineLock.readLock().lock();
    try {
      if (active) {
        if (period > 0)
          timer.schedule(task, delay, period);
        else
          timer.schedule(task, delay);
      } else
        OLogManager.instance().warn(this, "OrientDB engine is down. Task will not be scheduled.");
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public void scheduleTask(final TimerTask task, final Date firstTime, final long period) {
    engineLock.readLock().lock();
    try {
      if (active)
        if (period > 0)
          timer.schedule(task, firstTime, period);
        else
          timer.schedule(task, firstTime);
      else
        OLogManager.instance().warn(this, "OrientDB engine is down. Task will not be scheduled.");
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public void closeAllStorages() {
    shutdownAllStorages();
  }

  private void shutdownAllStorages() {
    engineLock.writeLock().lock();
    try {
      // CLOSE ALL THE STORAGES
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
    } finally {
      engineLock.writeLock().unlock();
    }
  }

  public boolean isActive() {
    return active;
  }

  /**
   * @deprecated This method is not thread safe. Use {@link #submit(java.util.concurrent.Callable)} instead.
   */
  @Deprecated
  public ThreadPoolExecutor getWorkers() {
    return workers;
  }

  public Future<?> submit(final Runnable runnable) {
    engineLock.readLock().lock();
    try {
      if (active)
        return workers.submit(runnable);
      else {
        OLogManager.instance().warn(this, "OrientDB engine is down. Task will not be submitted.");
        throw new IllegalStateException("OrientDB engine is down. Task will not be submitted.");
      }
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public <V> Future<V> submit(final Callable<V> callable) {
    engineLock.readLock().lock();
    try {
      if (active)
        return workers.submit(callable);
      else {
        OLogManager.instance().warn(this, "OrientDB engine is down. Task will not be submitted.");
        throw new IllegalStateException("OrientDB engine is down. Task will not be submitted.");
      }
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public OStorage loadStorage(String iURL) {
    if (iURL == null || iURL.length() == 0)
      throw new IllegalArgumentException("URL missed");

    if (iURL.endsWith("/"))
      iURL = iURL.substring(0, iURL.length() - 1);

    // SEARCH FOR ENGINE
    int pos = iURL.indexOf(':');
    if (pos <= 0)
      throw new OConfigurationException(
          "Error in database URL: the engine was not specified. Syntax is: " + URL_SYNTAX + ". URL was: " + iURL);

    final String engineName = iURL.substring(0, pos);

    engineLock.readLock().lock();
    try {
      final OEngine engine = engines.get(engineName.toLowerCase());

      if (engine == null)
        throw new OConfigurationException("Error on opening database: the engine '" + engineName + "' was not found. URL was: "
            + iURL + ". Registered engines are: " + engines.keySet());

      if (!engine.isRunning()) {
        final List<String> knownEngines = new ArrayList<String>(engines.keySet());
        if (!startEngine(engine))
          throw new OConfigurationException(
              "Error on opening database: the engine '" + engineName + "' was unable to start. URL was: " + iURL
                  + ". Registered engines was: " + knownEngines);
      }

      // SEARCH FOR DB-NAME
      iURL = iURL.substring(pos + 1);

      if (isWindowsOS()) {
        // WINDOWS ONLY: REMOVE DOUBLE SLASHES NOT AS PREFIX (WINDOWS PATH COULD NEED STARTING FOR "\\". EXAMPLE: "\\mydrive\db").
        // AT
        // THIS LEVEL BACKSLASHES ARRIVES AS SLASHES
        iURL = iURL.charAt(0) + iURL.substring(1).replace("//", "/");
      } else
        // REMOVE ANY //
        iURL = iURL.replace("//", "/");

      pos = iURL.indexOf('?');

      Map<String, String> parameters = null;
      String dbPath;
      if (pos > 0) {
        dbPath = iURL.substring(0, pos);
        iURL = iURL.substring(pos + 1);

        // PARSE PARAMETERS
        parameters = new HashMap<String, String>();
        String[] pairs = iURL.split("&");
        String[] kv;
        for (String pair : pairs) {
          kv = pair.split("=");
          if (kv.length < 2)
            throw new OConfigurationException(
                "Error on opening database: parameter has no value. Syntax is: " + URL_SYNTAX + ". URL was: " + iURL);
          parameters.put(kv[0], kv[1]);
        }
      } else
        dbPath = iURL;

      if (registerDatabaseByPath) {
        try {
          dbPath = new File(dbPath).getCanonicalPath();
        } catch (IOException e) {
          // IGNORE IT
        }
      }

      final String dbName = registerDatabaseByPath ? dbPath : engine.getNameFromPath(dbPath);

      OStorage storage;
      // SEARCH IF ALREADY USED
      storage = storages.get(dbName);
      if (storage == null) {
        // NOT FOUND: CREATE IT

        do {
          storage = engine.createStorage(dbPath, parameters);
        } while ((storage instanceof OIdentifiableStorage)
            && storageIds.putIfAbsent(((OIdentifiableStorage) storage).getId(), Boolean.TRUE) != null);

        final OStorage oldStorage = storages.putIfAbsent(dbName, storage);
        if (oldStorage != null)
          storage = oldStorage;

        for (OOrientListener l : browseListeners())
          l.onStorageRegistered(storage);
      }

      return storage;
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public boolean isWindowsOS() {
    return os.contains("win");
  }

  public OStorage getStorage(final String dbName) {
    engineLock.readLock().lock();
    try {
      return storages.get(dbName);
    } finally {
      engineLock.readLock().unlock();
    }
  }

  protected void registerEngine(final OEngine iEngine) throws IllegalArgumentException {
    OEngine oEngine = engines.get(iEngine.getName());

    if (oEngine != null) {
      if (!oEngine.getClass().isAssignableFrom(iEngine.getClass())) {
        throw new IllegalArgumentException("Cannot replace storage " + iEngine.getName());
      }
    }
    engines.put(iEngine.getName(), iEngine);
  }

  /**
   * Returns the engine by its name.
   *
   * @param engineName
   *          Engine name to retrieve
   * @return OEngine instance of found, otherwise null
   */
  public OEngine getEngine(final String engineName) {
    engineLock.readLock().lock();
    try {
      return engines.get(engineName);
    } finally {
      engineLock.readLock().unlock();
    }
  }

  /**
   * Obtains an {@link OEngine engine} instance with the given {@code engineName}, if it is {@link OEngine#isRunning() running}.
   *
   * @param engineName the name of the engine to obtain.
   * @return the obtained engine instance or {@code null} if no such engine known or the engine is not running.
   */
  public OEngine getEngineIfRunning(final String engineName) {
    engineLock.readLock().lock();
    try {
      final OEngine engine = engines.get(engineName);
      return engine == null || !engine.isRunning() ? null : engine;
    } finally {
      engineLock.readLock().unlock();
    }
  }

  /**
   * Obtains a {@link OEngine#isRunning() running} {@link OEngine engine} instance with the given {@code engineName}.
   * If engine is not running, starts it.
   *
   * @param engineName the name of the engine to obtain.
   * @return the obtained running engine instance, never {@code null}.
   * @throws IllegalStateException if an engine with the given is not found or failed to start.
   */
  public OEngine getRunningEngine(final String engineName) {
    engineLock.readLock().lock();
    try {
      OEngine engine = engines.get(engineName);
      if (engine == null)
        throw new IllegalStateException("Engine '" + engineName + "' is not found.");

      if (!engine.isRunning() && !startEngine(engine))
        throw new IllegalStateException("Engine '" + engineName + "' is failed to start.");

      return engine;
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public Set<String> getEngines() {
    engineLock.readLock().lock();
    try {
      return Collections.unmodifiableSet(engines.keySet());
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public void unregisterStorageByName(final String name) {
    final String dbName = registerDatabaseByPath ? name : OIOUtils.getRelativePathIfAny(name, null);
    final OStorage stg = storages.get(dbName);
    unregisterStorage(stg);
  }

  public void unregisterStorage(final OStorage storage) {
    if (!active)
      // SHUTDOWNING OR NOT ACTIVE: RETURN
      return;

    if (storage == null)
      return;

    engineLock.writeLock().lock();
    try {
      // UNREGISTER ALL THE LISTENER ONE BY ONE AVOIDING SELF-RECURSION BY REMOVING FROM THE LIST
      final Iterable<OOrientListener> listenerCopy = getListenersCopy();
      for (final OOrientListener l : listenerCopy) {
        unregisterListener(l);
        l.onStorageUnregistered(storage);
      }

      final List<String> storagesToRemove = new ArrayList<String>();

      for (Entry<String, OStorage> s : storages.entrySet()) {
        if (s.getValue().equals(storage))
          storagesToRemove.add(s.getKey());
      }

      for (String dbName : storagesToRemove)
        storages.remove(dbName);

      // UNREGISTER STORAGE FROM ENGINES IN CASE IS CACHED
      for (OEngine engine : engines.values()) {
        engine.removeStorage(storage);
      }

    } finally {
      engineLock.writeLock().unlock();
    }
  }

  public Collection<OStorage> getStorages() {
    engineLock.readLock().lock();
    try {
      return new ArrayList<OStorage>(storages.values());
    } finally {
      engineLock.readLock().unlock();
    }
  }

  /**
   * @deprecated This method is not thread safe please use {@link #scheduleTask(java.util.TimerTask, long, long)} instead.
   */
  @Deprecated
  public Timer getTimer() {
    return timer;
  }

  public void removeShutdownHook() {
    if (shutdownHook != null) {
      shutdownHook.cancel();
      shutdownHook = null;
    }
  }

  public void removeSignalHandler() {
    if (signalHandler != null) {
      signalHandler.cancel();
      signalHandler = null;
    }
  }

  public boolean isSelfManagedShutdown() {
    return shutdownHook != null;
  }

  public Iterator<ODatabaseLifecycleListener> getDbLifecycleListeners() {
    return new HashSet<ODatabaseLifecycleListener>(dbLifecycleListeners.keySet()).iterator();
  }

  public void addDbLifecycleListener(final ODatabaseLifecycleListener iListener) {
    final Map<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY> tmp = new LinkedHashMap<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY>(
        dbLifecycleListeners);
    if (iListener.getPriority() == null)
      throw new IllegalArgumentException("Priority of DatabaseLifecycleListener '" + iListener + "' cannot be null");

    tmp.put(iListener, iListener.getPriority());
    dbLifecycleListeners.clear();
    for (ODatabaseLifecycleListener.PRIORITY p : ODatabaseLifecycleListener.PRIORITY.values()) {
      for (Map.Entry<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY> e : tmp.entrySet()) {
        if (e.getValue() == p)
          dbLifecycleListeners.put(e.getKey(), e.getValue());
      }
    }
  }

  public void removeDbLifecycleListener(final ODatabaseLifecycleListener iListener) {
    dbLifecycleListeners.remove(iListener);
  }

  public ThreadGroup getThreadGroup() {
    return threadGroup;
  }

  public ODatabaseThreadLocalFactory getDatabaseThreadFactory() {
    return databaseThreadFactory;
  }

  public ORecordFactoryManager getRecordFactoryManager() {
    return recordFactoryManager;
  }

  public void setRecordFactoryManager(final ORecordFactoryManager iRecordFactoryManager) {
    recordFactoryManager = iRecordFactoryManager;
  }

  public OProfiler getProfiler() {
    return profiler;
  }

  public void setProfiler(final OProfiler iProfiler) {
    profiler = iProfiler;
  }

  public OSecuritySystem getSecurity() {
    return this.security;
  }

  public void setSecurity(final OSecuritySystem security) {
    this.security = security;
  }

  public void registerThreadDatabaseFactory(final ODatabaseThreadLocalFactory iDatabaseFactory) {
    databaseThreadFactory = iDatabaseFactory;
  }

  public OScriptManager getScriptManager() {
    return scriptManager;
  }

  @Override
  public void registerListener(OOrientListener listener) {
    if (listener instanceof OOrientStartupListener)
      registerOrientStartupListener((OOrientStartupListener) listener);

    super.registerListener(listener);
  }

  @Override
  public void unregisterListener(OOrientListener listener) {
    if (listener instanceof OOrientStartupListener)
      unregisterOrientStartupListener((OOrientStartupListener) listener);

    super.unregisterListener(listener);
  }

  public void registerOrientStartupListener(OOrientStartupListener listener) {
    startupListeners.add(listener);
  }

  public void registerWeakOrientStartupListener(OOrientStartupListener listener) {
    purgeWeakStartupListeners();
    weakStartupListeners.add(new WeakHashSetValueHolder<OOrientStartupListener>(listener, removedStartupListenersQueue));
  }

  public void unregisterOrientStartupListener(OOrientStartupListener listener) {
    startupListeners.remove(listener);
  }

  public void unregisterWeakOrientStartupListener(OOrientStartupListener listener) {
    purgeWeakStartupListeners();
    weakStartupListeners.remove(new WeakHashSetValueHolder<OOrientStartupListener>(listener, null));
  }

  public void registerWeakOrientShutdownListener(OOrientShutdownListener listener) {
    purgeWeakShutdownListeners();
    weakShutdownListeners.add(new WeakHashSetValueHolder<OOrientShutdownListener>(listener, removedShutdownListenersQueue));
  }

  public void unregisterWeakOrientShutdownListener(OOrientShutdownListener listener) {
    purgeWeakShutdownListeners();
    weakShutdownListeners.remove(new WeakHashSetValueHolder<OOrientShutdownListener>(listener, null));
  }

  @Override
  public void resetListeners() {
    super.resetListeners();

    weakShutdownListeners.clear();

    startupListeners.clear();
    weakStartupListeners.clear();
  }

  public OLocalRecordCacheFactory getLocalRecordCache() {
    return localRecordCache;
  }

  private void purgeWeakStartupListeners() {
    synchronized (removedStartupListenersQueue) {
      WeakHashSetValueHolder<OOrientStartupListener> ref = (WeakHashSetValueHolder<OOrientStartupListener>) removedStartupListenersQueue
          .poll();
      while (ref != null) {
        weakStartupListeners.remove(ref);
        ref = (WeakHashSetValueHolder<OOrientStartupListener>) removedStartupListenersQueue.poll();
      }

    }
  }

  private void purgeWeakShutdownListeners() {
    synchronized (removedShutdownListenersQueue) {
      WeakHashSetValueHolder<OOrientShutdownListener> ref = (WeakHashSetValueHolder<OOrientShutdownListener>) removedShutdownListenersQueue
          .poll();
      while (ref != null) {
        weakShutdownListeners.remove(ref);
        ref = (WeakHashSetValueHolder<OOrientShutdownListener>) removedShutdownListenersQueue.poll();
      }

    }
  }

  private boolean startEngine(OEngine engine) {
    final String name = engine.getName();

    try {
      engine.startup();
      return true;
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error during initialization of engine '%s', engine will be removed", e, name);

      try {
        engine.shutdown();
      } catch (Exception se) {
        OLogManager.instance().error(this, "Error during engine shutdown", se);
      }

      engines.remove(name);
    }

    return false;
  }

  /**
   * Shutdown thread group which is used in methods {@link #submit(Callable)} and {@link #submit(Runnable)}.
   */
  public class OShutdownWorkersHandler implements OShutdownHandler {
    @Override
    public int getPriority() {
      return SHUTDOWN_WORKERS_PRIORITY;
    }

    @Override
    public void shutdown() throws Exception {
      workers.shutdown();
      try {
        workers.awaitTermination(2, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Closes all storages and shutdown all engines.
   */
  public class OShutdownEnginesHandler implements OShutdownHandler {
    @Override
    public int getPriority() {
      return SHUTDOWN_ENGINES_PRIORITY;
    }

    @Override
    public void shutdown() throws Exception {
      shutdownAllStorages();

      // SHUTDOWN ENGINES
      for (OEngine engine : engines.values())
        if (engine.isRunning())
          engine.shutdown();
      engines.clear();

      OByteBufferPool.instance().verifyState();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Interrupts all threads in OrientDB thread group and stops timer is used in methods {@link #scheduleTask(TimerTask, Date, long)}
   * and {@link #scheduleTask(TimerTask, long, long)}.
   */
  private class OShutdownPendingThreadsHandler implements OShutdownHandler {
    @Override
    public int getPriority() {
      return SHUTDOWN_PENDING_THREADS_PRIORITY;
    }

    @Override
    public void shutdown() throws Exception {
      if (threadGroup != null)
        // STOP ALL THE PENDING THREADS
        threadGroup.interrupt();

      if (timer != null) {
        timer.cancel();
        timer = null;
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Shutdown OrientDB profiler.
   */
  private class OShutdownProfilerHandler implements OShutdownHandler {
    @Override
    public int getPriority() {
      return SHUTDOWN_PROFILER_PRIORITY;
    }

    @Override
    public void shutdown() throws Exception {
      // NOTE: DON'T REMOVE PROFILER TO AVOID NPE AROUND THE CODE IF ANY THREADS IS STILL WORKING
      profiler.shutdown();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Calls all shutdown listeners.
   */
  private class OShutdownCallListenersHandler implements OShutdownHandler {
    @Override
    public int getPriority() {
      return SHUTDOWN_CALL_LISTENERS;
    }

    @Override
    public void shutdown() throws Exception {
      purgeWeakShutdownListeners();
      for (final WeakHashSetValueHolder<OOrientShutdownListener> wl : weakShutdownListeners)
        try {
          if (wl != null) {
            final OOrientShutdownListener l = wl.get();
            if (l != null) {
              l.onShutdown();
            }
          }

        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during orient shutdown", e);
        }

      // CALL THE SHUTDOWN ON ALL THE LISTENERS
      for (OOrientListener l : browseListeners()) {
        if (l != null)
          try {
            l.onShutdown();
          } catch (Exception e) {
            OLogManager.instance().error(this, "Error during orient shutdown", e);
          }

      }

      System.gc();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  public boolean isRunningDistributed() {
    return runningDistributed;
  }

  public void setRunningDistributed(final boolean runningDistributed) {
    this.runningDistributed = runningDistributed;
  }
}
