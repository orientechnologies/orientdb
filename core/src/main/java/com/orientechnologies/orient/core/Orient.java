/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.engine.OEngine;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.record.ORecordFactoryManager;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OMMapManager;

public class Orient extends OSharedResourceAbstract {
	public static final String								URL_SYNTAX						= "<engine>:<db-type>:<db-name>[?<db-param>=<db-value>[&]]*";

	protected Map<String, OEngine>						engines								= new HashMap<String, OEngine>();
	protected Map<String, OStorage>						storages							= new HashMap<String, OStorage>();
	protected Set<ODatabaseLifecycleListener>	dbLifecycleListeners	= new HashSet<ODatabaseLifecycleListener>();
	protected final List<OOrientListener>			listeners							= new ArrayList<OOrientListener>();
	protected ORecordFactoryManager						recordFactoryManager	= new ORecordFactoryManager();
	protected volatile boolean								active								= false;

	protected static final OrientShutdownHook	shutdownHook					= new OrientShutdownHook();
	protected static final Timer							timer									= new Timer(true);
	protected static final ThreadGroup				threadGroup						= new ThreadGroup("OrientDB");
	protected static Orient										instance							= new Orient();

	private final OMemoryWatchDog							memoryWatchDog;
	private static AtomicInteger							serialId							= new AtomicInteger();

	protected Orient() {
		// REGISTER THE EMBEDDED ENGINE
		registerEngine(new OEngineLocal());
		registerEngine(new OEngineMemory());
		registerEngine("com.orientechnologies.orient.client.remote.OEngineRemote");

		if (OGlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean())
			// ACTIVATE RECORDING OF THE PROFILER
			OProfiler.getInstance().startRecording();

		memoryWatchDog = new OMemoryWatchDog(OGlobalConfiguration.MEMORY_OPTIMIZE_THRESHOLD.getValueAsFloat());

		active = true;
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

		acquireExclusiveLock();
		try {
			final OEngine engine = engines.get(engineName.toLowerCase());

			if (engine == null)
				throw new OConfigurationException("Error on opening database: the engine '" + engineName + "' was not found. URL was: "
						+ iURL + ". Registered engines are: " + engines.keySet());

			// SEARCH FOR DB-NAME
			iURL = iURL.substring(pos + 1);
			pos = iURL.indexOf('?');

			Map<String, String> parameters = null;
			String dbName = null;
			if (pos > 0) {
				dbName = iURL.substring(0, pos);
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
				dbName = iURL;

			OStorage storage;
			if (engine.isShared()) {
				// SEARCH IF ALREADY USED
				storage = storages.get(dbName);
				if (storage == null) {
					// NOT FOUND: CREATE IT
					storage = engine.createStorage(dbName, parameters);
					storages.put(dbName, storage);
				}
			} else {
				// REGISTER IT WITH A SERIAL NAME TO AVOID BEING REUSED
				storage = engine.createStorage(dbName, parameters);
				storages.put(dbName + "__" + serialId.incrementAndGet(), storage);
			}

			for (OOrientListener l : listeners)
				l.onStorageRegistered(storage);

			return storage;

		} finally {
			releaseExclusiveLock();
		}
	}

	public void registerStorage(final OStorage iStorage) throws IOException {
		for (OOrientListener l : listeners)
			l.onStorageRegistered(iStorage);

		acquireExclusiveLock();
		try {
			if (!storages.containsKey(iStorage.getName()))
				storages.put(iStorage.getURL(), iStorage);

		} finally {
			releaseExclusiveLock();
		}
	}

	public OStorage getStorage(final String iDbName) {
		acquireSharedLock();
		try {
			return storages.get(iDbName);
		} finally {
			releaseSharedLock();
		}
	}

	public void registerEngine(OEngine iEngine) {
		acquireExclusiveLock();
		try {
			engines.put(iEngine.getName(), iEngine);
		} finally {
			releaseExclusiveLock();
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
		acquireSharedLock();
		try {
			return engines.get(iEngineName);
		} finally {
			releaseSharedLock();
		}
	}

	public Set<String> getEngines() {
		acquireSharedLock();
		try {
			return Collections.unmodifiableSet(engines.keySet());
		} finally {
			releaseSharedLock();
		}
	}

	public void unregisterStorage(final OStorage iStorage) {
		for (OOrientListener l : listeners)
			l.onStorageUnregistered(iStorage);

		acquireExclusiveLock();
		try {
			for (Entry<String, OStorage> s : storages.entrySet()) {
				if (s.getValue() == iStorage) {
					storages.remove(s.getKey());
					break;
				}
			}
			//
			// for (Entry<String, OEngine> engine : engines.entrySet()) {
			// engine.getValue().removeStorage(iStorage);
			// }
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<OStorage> getStorages() {
		try {
			acquireSharedLock();

			return new ArrayList<OStorage>(storages.values());

		} finally {
			releaseSharedLock();
		}
	}

	public void shutdown() {
		acquireExclusiveLock();
		try {
			if (!active)
				return;

			OLogManager.instance().debug(this, "Orient Engine is shutting down...");

			// CLOSE ALL THE STORAGES
			final List<OStorage> storagesCopy = new ArrayList<OStorage>(storages.values());
			for (OStorage stg : storagesCopy) {
				OLogManager.instance().debug(this, "Shutting down storage: " + stg.getName() + "...");
				stg.close(true);
			}

			OMMapManager.shutdown();
			active = false;

			// STOP ALL THE PENDING THREADS
			threadGroup.interrupt();
			listeners.clear();

			OLogManager.instance().debug(this, "Orient Engine shutdown complete");

		} finally {
			releaseExclusiveLock();
		}
	}

	public static Timer getTimer() {
		return timer;
	}

	public void removeShutdownHook() {
		Runtime.getRuntime().removeShutdownHook(shutdownHook);
	}

	public Iterable<ODatabaseLifecycleListener> getDbLifecycleListeners() {
		return dbLifecycleListeners;
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

	public static ThreadGroup getThreadGroup() {
		return threadGroup;
	}

	public OMemoryWatchDog getMemoryWatchDog() {
		return memoryWatchDog;
	}

	public ORecordFactoryManager getRecordFactoryManager() {
		return recordFactoryManager;
	}

	public void registerListener(final OOrientListener iListener) {
		if (!listeners.contains(iListener))
			listeners.add(iListener);
	}

	public void unregisterListener(final OOrientListener iListener) {
		for (int i = 0; i < listeners.size(); ++i)
			if (listeners.get(i) == iListener) {
				listeners.remove(i);
				break;
			}
	}

	public List<OOrientListener> getListeners() {
		return listeners;
	}

}
