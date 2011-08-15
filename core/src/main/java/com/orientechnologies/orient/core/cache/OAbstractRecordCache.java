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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Cache of documents.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OAbstractRecordCache extends OSharedResourceAbstract {
	protected boolean				enabled	= true;
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

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnable(final boolean iValue) {
		enabled = iValue;
		if (!iValue)
			entries.clear();
	}

	public ORecordInternal<?> findRecord(final ORID iRid) {
		return null;
	}

	public ORecordInternal<?> freeRecord(final ORID iRID) {
		if (!enabled)
			// PRECONDITIONS
			return null;

		acquireExclusiveLock();
		try {
			return entries.remove(iRID);
		} finally {
			releaseExclusiveLock();
		}
	}

	public void freeCluster(final int clusterId) {
		if (!enabled)
			// PRECONDITIONS
			return;

		acquireExclusiveLock();
		final Set<ORID> toRemove = new HashSet<ORID>();
		try {
			for (ORID entry : entries.keySet()) {
				if (entry.getClusterId() == clusterId) {
					toRemove.add(entry);
				}
			}
			for (ORID ridToRemove : toRemove) {
				entries.remove(ridToRemove);
			}
		} finally {
			toRemove.clear();
			releaseExclusiveLock();
		}
	}

	/**
	 * Remove multiple records from the cache in one shot.
	 * 
	 * @param iRecords
	 *          List of RIDs as RecordID instances
	 */
	public void removeRecords(final Collection<ORID> iRecords) {
		if (!enabled)
			// PRECONDITIONS
			return;

		acquireExclusiveLock();
		try {
			for (ORID id : iRecords)
				entries.remove(id);
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Delete a record entry from both database and storage caches.
	 * 
	 * @param iRecord
	 *          Record to remove
	 */
	public void deleteRecord(final ORID iRecord) {
		if (!enabled)
			// PRECONDITIONS
			return;

		acquireExclusiveLock();
		try {
			entries.remove(iRecord);
		} finally {
			releaseExclusiveLock();
		}
	}

	public boolean existsRecord(final ORID iRID) {
		if (!enabled)
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
	 * Clear the entire cache by removing all the entries.
	 */
	public void clear() {
		if (!enabled)
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

						final int threshold = (int) (oldSize * 0.9f);

						entries.removeEldestItems(threshold);

						OLogManager.instance().debug(this, "Low memory: auto reduce the record cache size from %d to %d", oldSize, threshold);
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
						OLogManager.instance().debug(this, "Clearing %d resources", entries.size());
						entries.clear();
					} catch (Exception e) {
						OLogManager.instance().error(this, "Error while freeing resources", e);
					} finally {
						releaseExclusiveLock();
					}
				}
			}
		});

		OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.enabled", new OProfilerHookValue() {
			public Object getValue() {
				return enabled;
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
