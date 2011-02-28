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

import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Cache of documents.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("serial")
public abstract class OAbstractRecordCache extends OSharedResource {
	protected int																			maxSize;
	protected LinkedHashMap<ORID, ORecordInternal<?>>	entries;

	protected Listener																watchDogListener;
	protected String																	profilerPrefix;

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
		entries = new LinkedHashMap<ORID, ORecordInternal<?>>(initialSize, 0.75f, true) {
			@Override
			protected boolean removeEldestEntry(Map.Entry<ORID, ORecordInternal<?>> iEldest) {
				return maxSize > -1 && size() > maxSize;
			}
		};
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
			 * Auto reduce cache size of 10%
			 */
			public void memoryUsageLow(TYPE iType, final long usedMemory, final long maxMemory) {
				if (iType == TYPE.JVM) {

					acquireExclusiveLock();

					final int oldSize = entries.size();
					if (oldSize == 0)
						// UNACTIVE
						return;

					final int threshold = (int) (maxSize > -1 ? maxSize * 0.7f : oldSize * 0.7f);

					try {
						if (entries.size() < threshold)
							return;

						// CLEAR THE CACHE: USE A TEMP ARRAY TO AVOID ITERATOR EXCEPTIONS
						final ORID[] ridToRemove = new ORID[entries.size() - threshold];
						int i = 0;

						for (ORID rid : entries.keySet()) {
							ridToRemove[i++] = rid;

							if (i >= ridToRemove.length)
								break;
						}

						for (ORID rid : ridToRemove)
							entries.remove(rid);

					} finally {
						System.err.println("Size after free: " + entries.size());
						releaseExclusiveLock();
					}

					OLogManager.instance().debug(this, "Low memory: auto reduce the record cache size from %d to %d", oldSize, threshold);
					maxSize = threshold;
				}
			}
		});

		OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.current", new OProfilerHookValue() {
			public Object getValue() {
				return entries.size();
			}
		});

		OProfiler.getInstance().registerHookValue(profilerPrefix + ".cache.max", new OProfilerHookValue() {
			public Object getValue() {
				return maxSize;
			}
		});
	}
}
