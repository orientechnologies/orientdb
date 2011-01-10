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
import java.util.LinkedHashMap;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/**
 * Per-database cache containing all the record buffers parked in memory to improve access. The cache is of type LRU to keep the
 * Last Recent records in memory and evict the others.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("serial")
public class OCacheRecord extends OSharedResourceAdaptive {
	private int																maxSize;
	private LinkedHashMap<String, ORawBuffer>	cache;

	/**
	 * Create the cache of iMaxSize size.
	 * 
	 * @param iMaxSize
	 *          Maximum number of elements for the cache
	 */
	public OCacheRecord() {
		maxSize = OGlobalConfiguration.STORAGE_CACHE_SIZE.getValueAsInteger();

		cache = new LinkedHashMap<String, ORawBuffer>(maxSize, 0.75f, true) {
			@Override
			protected boolean removeEldestEntry(java.util.Map.Entry<String, ORawBuffer> iEldest) {
				return size() > maxSize;
			}
		};

		Orient.instance().getMemoryWatchDog().addListener(new Listener() {
			/**
			 * Auto reduce cache size of 10%
			 */
			public void memoryUsageLow(final long usedMemory, final long maxMemory) {
				final int threshold = maxSize * 90 / 100;

				if (cache.size() < threshold)
					return;

				OLogManager.instance().debug(this, "Low memory: auto reduce the storage cache size from %d to %d", maxSize, threshold);
				maxSize = threshold;
			}
		});
	}

	public void pushRecord(final String iRecord, ORawBuffer iContent) {
		if (maxSize == 0)
			return;

		final boolean locked = acquireExclusiveLock();

		try {
			if (iContent == null || iContent.buffer == null || iContent.buffer.length == 0)
				// NULL RECORD: REMOVE FROM THE CACHE TO SAVE SPACE
				cache.remove(iRecord);
			else
				cache.put(iRecord, iContent);

		} finally {
			releaseExclusiveLock(locked);
		}
	}

	/**
	 * Find a record in cache by String
	 * 
	 * @param iRecord
	 *          String instance
	 * @return The record buffer if found, otherwise null
	 */
	public ORawBuffer getRecord(final String iRecord) {
		if (maxSize == 0)
			return null;

		final boolean locked = acquireSharedLock();

		try {
			return cache.get(iRecord);

		} finally {
			releaseSharedLock(locked);
		}
	}

	public ORawBuffer popRecord(final String iRecord) {
		if (maxSize == 0)
			return null;

		final boolean locked = acquireExclusiveLock();

		try {
			ORawBuffer buffer = cache.remove(iRecord);

			if (buffer != null)
				OProfiler.getInstance().updateCounter("Cache.reused", +1);

			return buffer;
		} finally {
			releaseExclusiveLock(locked);
		}
	}

	public void removeRecord(final String iRecord) {
		if (maxSize == 0)
			return;

		final boolean locked = acquireExclusiveLock();

		try {
			cache.remove(iRecord);

		} finally {
			releaseExclusiveLock(locked);
		}
	}

	/**
	 * Remove multiple records from the cache in one shot saving the cost of locking for each record.
	 * 
	 * @param iRecords
	 *          List of RIDs as RecordID instances or Strings
	 */
	public void removeRecords(final Collection<?> iRecords) {
		if (maxSize == 0)
			return;

		final boolean locked = acquireExclusiveLock();

		try {
			for (Object id : iRecords)
				cache.remove(id.toString());

		} finally {
			releaseExclusiveLock(locked);
		}
	}

	public void clear() {
		if (maxSize == 0)
			return;

		final boolean locked = acquireExclusiveLock();

		try {
			cache.clear();

		} finally {
			releaseExclusiveLock(locked);
		}
	}

	public int getMaxSize() {
		return maxSize;
	}

	public int size() {
		final boolean locked = acquireSharedLock();

		try {
			return cache.size();

		} finally {
			releaseSharedLock(locked);
		}
	}

	@Override
	public String toString() {
		return "Cached items=" + cache.size() + ", maxSize=" + maxSize;
	}

}
