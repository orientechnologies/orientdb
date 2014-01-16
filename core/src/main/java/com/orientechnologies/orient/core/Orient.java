/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.common.concur.lock.OAdaptiveLock;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseFactory;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseThreadLocalFactory;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.record.ORecordFactoryManager;
import com.orientechnologies.orient.core.storage.OClusterFactory;
import com.orientechnologies.orient.core.storage.ODefaultClusterFactory;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OMMapManagerLocator;

public class Orient extends OListenerManger<OOrientListener> {
  public static final String                      ORIENTDB_HOME          = "ORIENTDB_HOME";
  public static final String                      URL_SYNTAX             = "<engine>:<db-type>:<db-name>[?<db-param>=<db-value>[&]]*";

  protected static final Orient                   instance               = new Orient();
  protected static boolean                        registerDatabaseByPath = false;

  protected final Map<String, OEngine>            engines                = new HashMap<String, OEngine>();
  protected final Map<String, OStorage>           storages               = new HashMap<String, OStorage>();
  protected final Set<ODatabaseLifecycleListener> dbLifecycleListeners   = new HashSet<ODatabaseLifecycleListener>();
  protected final ODatabaseFactory                databaseFactory        = new ODatabaseFactory();
  protected final OScriptManager                  scriptManager          = new OScriptManager();
  protected OClusterFactory                       clusterFactory         = new ODefaultClusterFactory();
  protected ORecordFactoryManager                 recordFactoryManager   = new ORecordFactoryManager();

  protected OrientShutdownHook                    shutdownHook;
  protected final Timer                           timer                  = new Timer(true);
  protected final ThreadGroup                     threadGroup            = new ThreadGroup("OrientDB");
  protected final AtomicInteger                   serialId               = new AtomicInteger();

  protected OMemoryWatchDog                       memoryWatchDog;
  protected OProfilerMBean                        profiler               = new OProfiler();                                            ;

  protected ODatabaseThreadLocalFactory           databaseThreadFactory;

  protected volatile boolean                      active                 = false;

  protected Orient() {
    super(new OAdaptiveLock(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean()));
    startup();
  }

  public Orient startup() {
    getLock().lock();
    try {
      if (active)
        // ALREADY ACTIVE
        return this;

      shutdownHook = new OrientShutdownHook();

      // REGISTER THE EMBEDDED ENGINE
      registerEngine(new OEngineLocal());
      registerEngine(new OEngineLocalPaginated());
      registerEngine(new OEngineMemory());
      registerEngine("com.orientechnologies.orient.client.remote.OEngineRemote");

      if (OGlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean())
        // ACTIVATE RECORDING OF THE PROFILER
        profiler.startRecording();

      if (OGlobalConfiguration.ENVIRONMENT_DUMP_CFG_AT_STARTUP.getValueAsBoolean())
        OGlobalConfiguration.dumpConfiguration(System.out);

      memoryWatchDog = new OMemoryWatchDog();

      active = true;
      return this;

    } finally {
      getLock().unlock();
    }
  }

  public Orient shutdown() {
    getLock().lock();
    try {
      if (!active)
        return this;

      active = false;

      if (memoryWatchDog != null) {
        // SHUTDOWN IT AND WAIT FOR COMPLETITION
        memoryWatchDog.interrupt();
        try {
          memoryWatchDog.join();
        } catch (InterruptedException e) {
        }
      }

      if (shutdownHook != null) {
        shutdownHook.cancel();
        shutdownHook = null;
      }

      OLogManager.instance().debug(this, "Orient Engine is shutting down...");

      // CALL THE SHUTDOWN ON ALL THE LISTENERS
      for (OOrientListener l : browseListeners()) {
        if (l != null)
          l.onShutdown();
      }

      // SHUTDOWN ENGINES
      for (OEngine engine : engines.values())
        engine.shutdown();
      engines.clear();

      if (databaseFactory != null)
        // CLOSE ALL DATABASES
        databaseFactory.shutdown();

      if (storages != null) {
        // CLOSE ALL THE STORAGES
        final List<OStorage> storagesCopy = new ArrayList<OStorage>(storages.values());
        for (OStorage stg : storagesCopy) {
          OLogManager.instance().info(this, "Shutting down storage: " + stg.getName() + "...");
          stg.close(true);
        }
      }

      if (OMMapManagerLocator.getInstance() != null)
        OMMapManagerLocator.getInstance().shutdown();

      if (threadGroup != null)
        // STOP ALL THE PENDING THREADS
        threadGroup.interrupt();

      resetListeners();

      timer.purge();

      profiler.shutdown();

      OLogManager.instance().info(this, "Orient Engine shutdown complete\n");

    } finally {
      getLock().unlock();
    }
    return this;
  }

  public OStorage loadStorage(String iURL) {
    if (iURL == null || iURL.length() == 0)
      throw new IllegalArgumentException("URL missed");

    if (iURL.endsWith("/"))
      iURL = iURL.substring(0, iURL.length() - 1);

    // SEARCH FOR ENGINE
    int pos = iURL.indexOf(':');
    if (pos <= 0)
      throw new OConfigurationException("Error in database URL: the engine was not specified. Syntax is: " + URL_SYNTAX
          + ". URL was: " + iURL);

    final String engineName = iURL.substring(0, pos);

    getLock().lock();
    try {
      final OEngine engine = engines.get(engineName.toLowerCase());

      if (engine == null)
        throw new OConfigurationException("Error on opening database: the engine '" + engineName + "' was not found. URL was: "
            + iURL + ". Registered engines are: " + engines.keySet());

      // SEARCH FOR DB-NAME
      iURL = iURL.substring(pos + 1);
      pos = iURL.indexOf('?');

      Map<String, String> parameters = null;
      String dbPath = null;
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
            throw new OConfigurationException("Error on opening database: parameter has no value. Syntax is: " + URL_SYNTAX
                + ". URL was: " + iURL);
          parameters.put(kv[0], kv[1]);
        }
      } else
        dbPath = iURL;

      final String dbName = registerDatabaseByPath ? dbPath : OIOUtils.getRelativePathIfAny(dbPath, null);

      OStorage storage;
      if (engine.isShared()) {
        // SEARCH IF ALREADY USED
        storage = storages.get(dbName);
        if (storage == null) {
          // NOT FOUND: CREATE IT
          storage = engine.createStorage(dbPath, parameters);
          storages.put(dbName, storage);
        }
      } else {
        // REGISTER IT WITH A SERIAL NAME TO AVOID BEING REUSED
        storage = engine.createStorage(dbPath, parameters);
        storages.put(dbName + "__" + serialId.incrementAndGet(), storage);
      }

      for (OOrientListener l : browseListeners())
        l.onStorageRegistered(storage);

      return storage;

    } finally {
      getLock().unlock();
    }
  }

  public OStorage registerStorage(final OStorage iStorage) throws IOException {
    getLock().lock();
    try {
      for (OOrientListener l : browseListeners())
        l.onStorageRegistered(iStorage);

      if (!storages.containsKey(iStorage.getName()))
        storages.put(iStorage.getName(), iStorage);

    } finally {
      getLock().unlock();
    }
    return iStorage;
  }

  public OStorage getStorage(final String iDbName) {
    getLock().lock();
    try {
      return storages.get(iDbName);
    } finally {
      getLock().unlock();
    }
  }

  public void registerEngine(final OEngine iEngine) {
    getLock().lock();
    try {
      engines.put(iEngine.getName(), iEngine);
    } finally {
      getLock().unlock();
    }
  }

  private void registerEngine(final String iClassName) {
    try {
      final Class<?> cls = Class.forName(iClassName);
      registerEngine((OEngine) cls.newInstance());
    } catch (Exception e) {
    }
  }

  /**
   * Returns the engine by its name.
   * 
   * @param iEngineName
   *          Engine name to retrieve
   * @return OEngine instance of found, otherwise null
   */
  public OEngine getEngine(final String iEngineName) {
    getLock().lock();
    try {
      return engines.get(iEngineName);
    } finally {
      getLock().unlock();
    }
  }

  public Set<String> getEngines() {
    getLock().lock();
    try {
      return Collections.unmodifiableSet(engines.keySet());
    } finally {
      getLock().unlock();
    }
  }

  public void unregisterStorageByName(final String iName) {
    final String dbName = registerDatabaseByPath ? iName : OIOUtils.getRelativePathIfAny(iName, null);
    final OStorage stg = storages.get(dbName);
    unregisterStorage(stg);
  }

  public void unregisterStorage(final OStorage iStorage) {
    if (!active)
      // SHUTDOWNING OR NOT ACTIVE: RETURN
      return;

    if (iStorage == null)
      return;

    getLock().lock();
    try {
      // UNREGISTER ALL THE LISTENER ONE BY ONE AVOIDING SELF-RECURSION BY REMOVING FROM THE LIST
      final Iterable<OOrientListener> listenerCopy = getListenersCopy();
      for (Iterator<OOrientListener> it = listenerCopy.iterator(); it.hasNext();) {
        final OOrientListener l = it.next();
        unregisterListener(l);
        l.onStorageUnregistered(iStorage);
      }

      for (Entry<String, OStorage> s : storages.entrySet()) {
        if (s.getValue().equals(iStorage)) {
          storages.remove(s.getKey());
          break;
        }
      }
    } finally {
      getLock().unlock();
    }
  }

  public Collection<OStorage> getStorages() {
    getLock().lock();
    try {

      return new ArrayList<OStorage>(storages.values());

    } finally {
      getLock().unlock();
    }
  }

  public Timer getTimer() {
    return timer;
  }

  public void removeShutdownHook() {
    if (shutdownHook != null)
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
  }

  public Iterator<ODatabaseLifecycleListener> getDbLifecycleListeners() {
    return dbLifecycleListeners.iterator();
  }

  public void addDbLifecycleListener(final ODatabaseLifecycleListener iListener) {
    dbLifecycleListeners.add(iListener);
  }

  public void removeDbLifecycleListener(final ODatabaseLifecycleListener iListener) {
    dbLifecycleListeners.remove(iListener);
  }

  public static Orient instance() {
    return instance;
  }

  public ThreadGroup getThreadGroup() {
    return threadGroup;
  }

  public ODatabaseThreadLocalFactory getDatabaseThreadFactory() {
    return databaseThreadFactory;
  }

  public OMemoryWatchDog getMemoryWatchDog() {
    return memoryWatchDog;
  }

  public ORecordFactoryManager getRecordFactoryManager() {
    return recordFactoryManager;
  }

  public OClusterFactory getClusterFactory() {
    return clusterFactory;
  }

  public ODatabaseFactory getDatabaseFactory() {
    return databaseFactory;
  }

  public void setRecordFactoryManager(final ORecordFactoryManager iRecordFactoryManager) {
    recordFactoryManager = iRecordFactoryManager;
  }

  public static String getHomePath() {
    String v = System.getProperty("orient.home");
    if (v == null)
      v = System.getProperty(ORIENTDB_HOME);
    if (v == null)
      v = System.getenv(ORIENTDB_HOME);

    return v;
  }

  public void setClusterFactory(final OClusterFactory clusterFactory) {
    this.clusterFactory = clusterFactory;
  }

  public OProfilerMBean getProfiler() {
    return profiler;
  }

  public void registerThreadDatabaseFactory(final ODatabaseThreadLocalFactory iDatabaseFactory) {
    databaseThreadFactory = iDatabaseFactory;
  }

  public OScriptManager getScriptManager() {
    return scriptManager;
  }

  /**
   * Tells if to register database by path. Default is false. Setting to true allows to have multiple databases in different path
   * with the same name.
   * 
   * @see #setRegisterDatabaseByPath(boolean)
   * @return
   */
  public static boolean isRegisterDatabaseByPath() {
    return registerDatabaseByPath;
  }

  /**
   * Register database by path. Default is false. Setting to true allows to have multiple databases in different path with the same
   * name.
   * 
   * @param iValue
   */
  public static void setRegisterDatabaseByPath(final boolean iValue) {
    registerDatabaseByPath = iValue;
  }

  public void setProfiler(final OProfilerMBean iProfiler) {
    profiler = iProfiler;
  }
}
