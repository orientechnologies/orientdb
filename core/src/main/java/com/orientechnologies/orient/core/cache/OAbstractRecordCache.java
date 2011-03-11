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
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORID;

/**
 * Cache of documents.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OAbstractRecordCache extends OSharedResource {
	protected int						maxSize;
	protected ORecordCache	entries;

	protected Listener			watchDogListener;
	protected String				profilerPrefix;

	/**
	 * Create the cache of iMaxSize size.
	 * 
	 * @param iMaxSize
	 *          Maximum number of elements for the cache
	 */
	public OAbstractRecordCache(final String iProfilerPrefix, final int iMaxSize) {
		profilerPrefix = iProfilerPrefix;
		maxSize = iMaxSize;

		final int initialSize = maxSize > -1 ? maxSize + 1 : 1000;
		entries = new ORecordCache(maxSize, initialSize, 0.75f);
	}

	public boolean existsRecord(final ORID iRID) {
		if (maxSize == 0)
			// PRECONDITIONS
			return false;

		acquireSharedLock();
		try {
			return entries.containsKey(iRID);
		} finally {
			releaseSharedLock();
		}
	}

	/**
	 * Remove a record from the cache.
	 * 
	 * @param iRID
	 *          RecordID to remove
	 */
	public void removeRecord(final ORID iRID) {
		if (maxSize == 0)
			// PRECONDITIONS
			return;

		acquireExclusiveLock();
		try {
			entries.remove(iRID);

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Clear the entire cache by removing all the entries.
	 */
	public void clear() {
		if (maxSize == 0)
			// PRECONDITIONS
			return;

		acquireExclusiveLock();
		try {
			entries.clear();

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Return the total cached entries.
	 * 
	 * @return
	 */
	public int getSize() {
		acquireSharedLock();
		try {
			return entries.size();

		} finally {
			releaseSharedLock();
		}
	}

	public int getMaxSize() {
		acquireSharedLock();
		try {
			return maxSize;

		} finally {
			releaseSharedLock();
		}
	}

	public void shutdown() {
		acquireExclusiveLock();

		try {
			entries.clear();
			Orient.instance().getMemoryWatchDog().removeListener(watchDogListener);
			watchDogListener = null;

		} finally {
			releaseExclusiveLock();
		}
	}

	public void setMaxSize(final int iMaxSize) {
		maxSize = iMaxSize;
	}

	public void startup() {
		watchDogListener = Orient.instance().getMemoryWatchDog().addListener(new Listener() {
			/**
			 * Auto reduce cache size of 50%
			 */
			public void memoryUsageLow(TYPE iType, final long usedMemory, final long maxMemory) {
				if (iType == TYPE.JVM) {
					acquireExclusiveLock();
					try {
						final int oldSize = entries.size();
						if (oldSize == 0)
							// UNACTIVE
							return;

						final int threshold = (int) (oldSize * 0.5f);

						entries.removeEldestItems(threshold);

						OLogManager.instance().warn(this, "Low memory: auto reduce the record cache size from %d to %d", oldSize, threshold);
					} catch (Exception e) {
						OLogManager.instance().error(this, "Error while freeing resources", e);
					} finally {
						releaseExclusiveLock();
					}
				}
			}

			/**
			 * Free the entire cache
			 */
			public void memoryUsageCritical(TYPE iType, final long usedMemory, final long maxMemory) {
				if (iType == TYPE.JVM) {
					acquireExclusiveLock();
					try {
						entries.clear();
					} catch (Exception e) {
						OLogManager.instance().error(this, "Error while freeing resources", e);
					} finally {
						releaseExclusiveLock();
					}
				}
			}
		});

		OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.current", new OProfilerHookValue() {
			public Object getValue() {
				acquireSharedLock();
				try {
					return entries.size();
				} finally {
					releaseSharedLock();
				}
			}
		});

		OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.max", new OProfilerHookValue() {
			public Object getValue() {
				return maxSize;
			}
		});
	}
}
