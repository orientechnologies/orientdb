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
package com.orientechnologies.orient.core;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerStub;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.common.util.OClassLoaderHelper;
import com.orientechnologies.orient.core.cache.OLocalRecordCacheFactory;
import com.orientechnologies.orient.core.cache.OLocalRecordCacheFactoryImpl;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategyFactory;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseThreadLocalFactory;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.record.ORecordFactoryManager;
import com.orientechnologies.orient.core.shutdown.OShutdownHandler;
import com.orientechnologies.orient.core.storage.OStorage;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Orient extends OListenerManger<OOrientListener> {
  public static final String ORIENTDB_HOME = "ORIENTDB_HOME";
  public static final String URL_SYNTAX =
      "<engine>:<db-type>:<db-name>[?<db-param>=<db-value>[&]]*";

  private static volatile Orient instance;
  private static final Lock initLock = new ReentrantLock();

  private static volatile boolean registerDatabaseByPath = false;

  private final ConcurrentMap<String, OEngine> engines = new ConcurrentHashMap<String, OEngine>();

  private final Map<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY>
      dbLifecycleListeners =
          new LinkedHashMap<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY>();
  private final ThreadGroup threadGroup;
  private final ReadWriteLock engineLock = new ReentrantReadWriteLock();
  private final ORecordConflictStrategyFactory recordConflictStrategy =
      new ORecordConflictStrategyFactory();
  private final ReferenceQueue<OOrientStartupListener> removedStartupListenersQueue =
      new ReferenceQueue<OOrientStartupListener>();
  private final ReferenceQueue<OOrientShutdownListener> removedShutdownListenersQueue =
      new ReferenceQueue<OOrientShutdownListener>();
  private final Set<OOrientStartupListener> startupListeners =
      Collections.newSetFromMap(new ConcurrentHashMap<OOrientStartupListener, Boolean>());
  private final Set<WeakHashSetValueHolder<OOrientStartupListener>> weakStartupListeners =
      Collections.newSetFromMap(
          new ConcurrentHashMap<WeakHashSetValueHolder<OOrientStartupListener>, Boolean>());
  private final Set<WeakHashSetValueHolder<OOrientShutdownListener>> weakShutdownListeners =
      Collections.newSetFromMap(
          new ConcurrentHashMap<WeakHashSetValueHolder<OOrientShutdownListener>, Boolean>());

  private final PriorityQueue<OShutdownHandler> shutdownHandlers =
      new PriorityQueue<OShutdownHandler>(
          11,
          new Comparator<OShutdownHandler>() {
            @Override
            public int compare(OShutdownHandler handlerOne, OShutdownHandler handlerTwo) {
              if (handlerOne.getPriority() > handlerTwo.getPriority()) return 1;

              if (handlerOne.getPriority() < handlerTwo.getPriority()) return -1;

              return 0;
            }
          });

  private final OLocalRecordCacheFactory localRecordCache = new OLocalRecordCacheFactoryImpl();

  private Set<OrientDBEmbedded> factories = Collections.newSetFromMap(new ConcurrentHashMap<>());

  private Set<OrientDBInternal> runningInstances = new HashSet<>();

  private final String os;

  private volatile Timer timer;
  private volatile ORecordFactoryManager recordFactoryManager = new ORecordFactoryManager();
  private OrientShutdownHook shutdownHook;
  private volatile OAbstractProfiler profiler;
  private ODatabaseThreadLocalFactory databaseThreadFactory;
  private volatile boolean active = false;
  private ExecutorService workers;
  private OSignalHandler signalHandler;

  /** Indicates that engine is initialized inside of web application container. */
  private volatile boolean insideWebContainer;

  /** Prevents duplications because of recursive initialization. */
  private static boolean initInProgress = false;

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
      if (this == o) return true;

      if (o == null || getClass() != o.getClass()) return false;

      WeakHashSetValueHolder that = (WeakHashSetValueHolder) o;

      if (hashCode != that.hashCode) return false;

      final T thisObject = get();
      final Object thatObject = that.get();

      if (thisObject == null && thatObject == null) return super.equals(that);
      else if (thisObject != null && thatObject != null) return thisObject.equals(thatObject);

      return false;
    }
  }

  Orient(boolean insideWebContainer) {
    super(true);
    this.insideWebContainer = insideWebContainer;
    this.os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);
    threadGroup = new ThreadGroup("OrientDB");
    threadGroup.setDaemon(false);
  }

  public boolean isInsideWebContainer() {
    return insideWebContainer;
  }

  public static Orient instance() {
    if (instance != null) return instance;

    return startUp(false);
  }

  public static Orient startUp(boolean insideWebContainer) {
    initLock.lock();
    try {
      if (initInProgress) {
        return null;
      }

      initInProgress = true;
      if (instance != null) return instance;

      final Orient orient = new Orient(insideWebContainer);
      orient.startup();

      instance = orient;
    } finally {
      initInProgress = false;
      initLock.unlock();
    }

    return instance;
  }

  public static String getHomePath() {
    String v = System.getProperty("orient.home");

    if (v == null) v = OSystemVariableResolver.resolveVariable(ORIENTDB_HOME);

    return OFileUtils.getPath(v);
  }

  public static String getTempPath() {
    return OFileUtils.getPath(System.getProperty("java.io.tmpdir") + "/orientdb/");
  }

  /**
   * Tells if to register database by path. Default is false. Setting to true allows to have
   * multiple databases in different path with the same name.
   *
   * @see #setRegisterDatabaseByPath(boolean)
   */
  public static boolean isRegisterDatabaseByPath() {
    return registerDatabaseByPath;
  }

  /**
   * Register database by path. Default is false. Setting to true allows to have multiple databases
   * in different path with the same name.
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

      if (timer == null) timer = new Timer(true);

      profiler = new OProfilerStub(false);

      shutdownHook = new OrientShutdownHook();
      if (signalHandler == null) {
        signalHandler = new OSignalHandler();
        signalHandler.installDefaultSignals();
      }

      final int cores = Runtime.getRuntime().availableProcessors();

      workers =
          OThreadPoolExecutors.newBlockingScalingThreadPool(
              "Orient Worker",
              threadGroup,
              cores,
              cores * 3,
              cores * 100,
              cores * 500,
              10,
              TimeUnit.SECONDS);

      registerEngines();

      if (OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP.getValueAsBoolean())
        OGlobalConfiguration.dumpConfiguration(System.out);

      active = true;

      for (OOrientStartupListener l : startupListeners)
        try {
          if (l != null) l.onStartup();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on startup", e);
        }

      purgeWeakStartupListeners();
      for (final WeakHashSetValueHolder<OOrientStartupListener> wl : weakStartupListeners)
        try {
          if (wl != null) {
            final OOrientStartupListener l = wl.get();
            if (l != null) l.onStartup();
          }

        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on startup", e);
        }

      initShutdownQueue();
      registerWeakOrientStartupListener(profiler);
    } finally {
      engineLock.writeLock().unlock();
    }

    return this;
  }

  /**
   * Add handler which will be executed during {@link #shutdown()} call.
   *
   * @param shutdownHandler Shutdown handler instance.
   */
  public void addShutdownHandler(OShutdownHandler shutdownHandler) {
    engineLock.writeLock().lock();
    try {
      shutdownHandlers.add(shutdownHandler);
    } finally {
      engineLock.writeLock().unlock();
    }
  }

  /** Adds shutdown handlers in order which will be used during execution of shutdown. */
  private void initShutdownQueue() {
    addShutdownHandler(new OShutdownWorkersHandler());
    addShutdownHandler(new OShutdownOrientDBInstancesHandler());
    addShutdownHandler(new OShutdownPendingThreadsHandler());
    addShutdownHandler(new OShutdownProfilerHandler());
    addShutdownHandler(new OShutdownCallListenersHandler());
  }

  /**
   * Shutdown whole OrientDB ecosystem. Usually is called during JVM shutdown by JVM shutdown
   * handler. During shutdown all handlers which were registered by the call of {@link
   * #addShutdownHandler(OShutdownHandler)} are called together with pre-registered system shoutdown
   * handlers according to their priority.
   *
   * @see OShutdownWorkersHandler
   * @see
   */
  private void registerEngines() {
    ClassLoader classLoader = Orient.class.getClassLoader();

    Iterator<OEngine> engines =
        OClassLoaderHelper.lookupProviderWithOrientClassLoader(OEngine.class, classLoader);

    OEngine engine = null;
    while (engines.hasNext()) {
      try {
        engine = engines.next();
        registerEngine(engine);
      } catch (IllegalArgumentException e) {
        if (engine != null)
          OLogManager.instance().debug(this, "Failed to replace engine " + engine.getName(), e);
      }
    }
  }

  public Orient shutdown() {
    engineLock.writeLock().lock();
    try {
      if (!active) return this;

      active = false;

      OLogManager.instance().info(this, "Orient Engine is shutting down...");
      for (OShutdownHandler handler : shutdownHandlers) {
        try {
          OLogManager.instance().debug(this, "Shutdown handler %s is going to be called", handler);
          handler.shutdown();
          OLogManager.instance().debug(this, "Shutdown handler %s completed", handler);
        } catch (Exception e) {
          OLogManager.instance()
              .error(this, "Exception during calling of shutdown handler %s", e, handler);
        }
      }

      shutdownHandlers.clear();

      OLogManager.instance().info(this, "Clearing byte buffer pool");
      OByteBufferPool.instance(null).clear();

      OByteBufferPool.instance(null).checkMemoryLeaks();
      ODirectMemoryAllocator.instance().checkMemoryLeaks();

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

  public TimerTask scheduleTask(final Runnable task, final long delay, final long period) {
    engineLock.readLock().lock();
    try {
      final TimerTask timerTask =
          new TimerTask() {
            @Override
            public void run() {
              try {
                task.run();
              } catch (Exception e) {
                OLogManager.instance()
                    .error(
                        this,
                        "Error during execution of task " + task.getClass().getSimpleName(),
                        e);
              } catch (Error e) {
                OLogManager.instance()
                    .error(
                        this,
                        "Error during execution of task " + task.getClass().getSimpleName(),
                        e);
                throw e;
              }
            }
          };

      if (active) {
        if (period > 0) {
          timer.schedule(timerTask, delay, period);
        } else {
          timer.schedule(timerTask, delay);
        }
      } else {
        OLogManager.instance().warn(this, "OrientDB engine is down. Task will not be scheduled.");
      }

      return timerTask;
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public TimerTask scheduleTask(final Runnable task, final Date firstTime, final long period) {
    engineLock.readLock().lock();
    try {
      final TimerTask timerTask =
          new TimerTask() {
            @Override
            public void run() {
              try {
                task.run();
              } catch (Exception e) {
                OLogManager.instance()
                    .error(
                        this,
                        "Error during execution of task " + task.getClass().getSimpleName(),
                        e);
              } catch (Error e) {
                OLogManager.instance()
                    .error(
                        this,
                        "Error during execution of task " + task.getClass().getSimpleName(),
                        e);
                throw e;
              }
            }
          };

      if (active) {
        if (period > 0) {
          timer.schedule(timerTask, firstTime, period);
        } else {
          timer.schedule(timerTask, firstTime);
        }
      } else {
        OLogManager.instance().warn(this, "OrientDB engine is down. Task will not be scheduled.");
      }

      return timerTask;
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public boolean isActive() {
    return active;
  }

  /**
   * @deprecated This method is not thread safe. Use {@link #submit(java.util.concurrent.Callable)}
   *     instead.
   */
  @Deprecated
  public ThreadPoolExecutor getWorkers() {
    return (ThreadPoolExecutor) workers;
  }

  public Future<?> submit(final Runnable runnable) {
    engineLock.readLock().lock();
    try {
      if (active) return workers.submit(runnable);
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
      if (active) return workers.submit(callable);
      else {
        OLogManager.instance().warn(this, "OrientDB engine is down. Task will not be submitted.");
        throw new IllegalStateException("OrientDB engine is down. Task will not be submitted.");
      }
    } finally {
      engineLock.readLock().unlock();
    }
  }

  public boolean isWindowsOS() {
    return os.contains("win");
  }

  public void registerEngine(final OEngine iEngine) throws IllegalArgumentException {
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
   * @param engineName Engine name to retrieve
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
   * Obtains an {@link OEngine engine} instance with the given {@code engineName}, if it is {@link
   * OEngine#isRunning() running}.
   *
   * @param engineName the name of the engine to obtain.
   * @return the obtained engine instance or {@code null} if no such engine known or the engine is
   *     not running.
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
   * Obtains a {@link OEngine#isRunning() running} {@link OEngine engine} instance with the given
   * {@code engineName}. If engine is not running, starts it.
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

  public Collection<OStorage> getStorages() {
    List<OStorage> storages = new ArrayList<>();
    for (OrientDBEmbedded factory : factories) {
      storages.addAll(factory.getStorages());
    }
    return storages;
  }

  public void removeShutdownHook() {
    if (shutdownHook != null) {
      shutdownHook.cancel();
      shutdownHook = null;
    }
  }

  public OSignalHandler getSignalHandler() {
    return signalHandler;
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
    return new LinkedHashSet<>(dbLifecycleListeners.keySet()).iterator();
  }

  public void addDbLifecycleListener(final ODatabaseLifecycleListener iListener) {
    final Map<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY> tmp =
        new LinkedHashMap<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY>(
            dbLifecycleListeners);
    if (iListener.getPriority() == null)
      throw new IllegalArgumentException(
          "Priority of DatabaseLifecycleListener '" + iListener + "' cannot be null");

    tmp.put(iListener, iListener.getPriority());
    dbLifecycleListeners.clear();
    for (ODatabaseLifecycleListener.PRIORITY p : ODatabaseLifecycleListener.PRIORITY.values()) {
      for (Map.Entry<ODatabaseLifecycleListener, ODatabaseLifecycleListener.PRIORITY> e :
          tmp.entrySet()) {
        if (e.getValue() == p) dbLifecycleListeners.put(e.getKey(), e.getValue());
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

  public void setProfiler(final OAbstractProfiler iProfiler) {
    profiler = iProfiler;
  }

  public void registerThreadDatabaseFactory(final ODatabaseThreadLocalFactory iDatabaseFactory) {
    databaseThreadFactory = iDatabaseFactory;
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
    weakStartupListeners.add(
        new WeakHashSetValueHolder<OOrientStartupListener>(listener, removedStartupListenersQueue));
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
    weakShutdownListeners.add(
        new WeakHashSetValueHolder<OOrientShutdownListener>(
            listener, removedShutdownListenersQueue));
  }

  public void unregisterWeakOrientShutdownListener(OOrientShutdownListener listener) {
    purgeWeakShutdownListeners();
    weakShutdownListeners.remove(
        new WeakHashSetValueHolder<OOrientShutdownListener>(listener, null));
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
      WeakHashSetValueHolder<OOrientStartupListener> ref =
          (WeakHashSetValueHolder<OOrientStartupListener>) removedStartupListenersQueue.poll();
      while (ref != null) {
        weakStartupListeners.remove(ref);
        ref = (WeakHashSetValueHolder<OOrientStartupListener>) removedStartupListenersQueue.poll();
      }
    }
  }

  private void purgeWeakShutdownListeners() {
    synchronized (removedShutdownListenersQueue) {
      WeakHashSetValueHolder<OOrientShutdownListener> ref =
          (WeakHashSetValueHolder<OOrientShutdownListener>) removedShutdownListenersQueue.poll();
      while (ref != null) {
        weakShutdownListeners.remove(ref);
        ref =
            (WeakHashSetValueHolder<OOrientShutdownListener>) removedShutdownListenersQueue.poll();
      }
    }
  }

  private boolean startEngine(OEngine engine) {
    final String name = engine.getName();

    try {
      engine.startup();
      return true;
    } catch (Exception e) {
      OLogManager.instance()
          .error(
              this, "Error during initialization of engine '%s', engine will be removed", e, name);

      try {
        engine.shutdown();
      } catch (Exception se) {
        OLogManager.instance().error(this, "Error during engine shutdown", se);
      }

      engines.remove(name);
    }

    return false;
  }

  /** Closes all storages and shutdown all engines. */
  public class OShutdownOrientDBInstancesHandler implements OShutdownHandler {
    @Override
    public int getPriority() {
      return SHUTDOWN_ENGINES_PRIORITY;
    }

    @Override
    public void shutdown() throws Exception {
      for (OrientDBInternal internal : runningInstances) {
        internal.internalClose();
      }
      runningInstances.clear();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Shutdown thread group which is used in methods {@link #submit(Callable)} and {@link
   * #submit(Runnable)}.
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
        OLogManager.instance().error(this, "Shutdown was interrupted", e);
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Interrupts all threads in OrientDB thread group and stops timer is used in methods {@link
   * #scheduleTask(Runnable, Date, long)} and {@link #scheduleTask(Runnable, long, long)}.
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
      // it is strange but windows defender block compilation if we get class name programmatically
      // using Class instance
      return "OShutdownPendingThreadsHandler";
    }
  }

  /** Shutdown OrientDB profiler. */
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

  /** Calls all shutdown listeners. */
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

  public void onEmbeddedFactoryInit(OrientDBEmbedded embeddedFactory) {
    OEngine memory = engines.get("memory");
    if (!memory.isRunning()) memory.startup();
    OEngine disc = engines.get("plocal");
    if (!disc.isRunning()) disc.startup();
    factories.add(embeddedFactory);
  }

  public void onEmbeddedFactoryClose(OrientDBEmbedded embeddedFactory) {
    factories.remove(embeddedFactory);
    if (factories.isEmpty()) {
      OEngine memory = engines.get("memory");
      if (memory.isRunning()) memory.shutdown();
      OEngine disc = engines.get("plocal");
      if (disc.isRunning()) disc.shutdown();
    }
  }

  public void addOrientDB(OrientDBInternal internal) {
    engineLock.writeLock().lock();
    try {
      runningInstances.add(internal);
    } finally {
      engineLock.writeLock().unlock();
    }
  }

  public void removeOrientDB(OrientDBInternal internal) {
    engineLock.writeLock().lock();
    try {
      runningInstances.remove(internal);
    } finally {
      engineLock.writeLock().unlock();
    }
  }
}
