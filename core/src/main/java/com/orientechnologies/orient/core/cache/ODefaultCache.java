/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Default implementation of generic {@link OCache} interface that uses plain {@link LinkedHashMap} to store records
 * 
 * @author Maxim Fedorov
 */
public class ODefaultCache implements OCache {
	private static final int							DEFAULT_LIMIT	= 1000;

	private final OSharedResourceExternal	lock					= new OSharedResourceExternal();
	private final AtomicBoolean						enabled				= new AtomicBoolean(false);

	private final OLinkedHashMapCache			cache;
	private final int											limit;

	protected OMemoryWatchDog.Listener		lowMemoryListener;

	public ODefaultCache(final int initialLimit) {
		final int initialCapacity = initialLimit > 0 ? initialLimit : DEFAULT_LIMIT;
		limit = initialLimit;
		cache = new OLinkedHashMapCache(initialCapacity, 0.75f, limit);
	}

	public void startup() {
		lowMemoryListener = Orient.instance().getMemoryWatchDog().addListener(new OLowMemoryListener());
		enable();
	}

	public void shutdown() {
		Orient.instance().getMemoryWatchDog().removeListener(lowMemoryListener);
		disable();
	}

	public boolean isEnabled() {
		return enabled.get();
	}

	public boolean enable() {
		return enabled.compareAndSet(false, true);
	}

	public boolean disable() {
		clear();
		return enabled.compareAndSet(true, false);
	}

	public ORecordInternal<?> get(ORID id) {
		if (!isEnabled())
			return null;
		try {
			lock.acquireSharedLock();
			return cache.get(id);
		} finally {
			lock.releaseSharedLock();
		}
	}

	public ORecordInternal<?> put(final ORecordInternal<?> record) {
		if (!isEnabled())
			return null;
		try {
			lock.acquireExclusiveLock();
			return cache.put(record.getIdentity(), record);
		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public ORecordInternal<?> remove(final ORID id) {
		if (!isEnabled())
			return null;
		try {
			lock.acquireExclusiveLock();
			return cache.remove(id);
		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void clear() {
		if (!isEnabled())
			return;
		try {
			lock.acquireExclusiveLock();
			cache.clear();
		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public int size() {
		try {
			lock.acquireSharedLock();
			return cache.size();
		} finally {
			lock.releaseSharedLock();
		}
	}

	public int limit() {
		return limit;
	}

	public Collection<ORID> keys() {
		try {
			lock.acquireSharedLock();
			return new ArrayList<ORID>(cache.keySet());
		} finally {
			lock.releaseSharedLock();
		}
	}

	private void removeEldest(final int threshold) {
		try {
			lock.acquireExclusiveLock();
			cache.removeEldest(threshold);
		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public void lock(ORID id) {
		lock.acquireExclusiveLock();
	}

	public void unlock(ORID id) {
		lock.releaseExclusiveLock();
	}

	/**
	 * Implementation of {@link LinkedHashMap} that will remove eldest entries if size limit will be exceeded.
	 * 
	 * @author Luca Garulli
	 */
	@SuppressWarnings("serial")
	static final class OLinkedHashMapCache extends LinkedHashMap<ORID, ORecordInternal<?>> {
		private final int	limit;

		public OLinkedHashMapCache(final int initialCapacity, final float loadFactor, final int limit) {
			super(initialCapacity, loadFactor, true);
			this.limit = limit;
		}

		@Override
		protected boolean removeEldestEntry(final Map.Entry<ORID, ORecordInternal<?>> eldest) {
			return limit > 0 ? size() - limit > 0 : false;
		}

		void removeEldest(final int amount) {
			final ORID[] victims = new ORID[amount];

			final int skip = size() - amount;
			int skipped = 0;
			int selected = 0;

			for (Map.Entry<ORID, ORecordInternal<?>> entry : entrySet()) {
				if (entry.getValue().isDirty() || entry.getValue().isPinned() == Boolean.TRUE || skipped++ < skip)
					continue;
				victims[selected++] = entry.getKey();
			}

			for (ORID id : victims)
				remove(id);
		}
	}

	class OLowMemoryListener implements OMemoryWatchDog.Listener {
		public void memoryUsageLow(final long freeMemory, final long freeMemoryPercentage) {
			try {
				final int oldSize = size();
				if (oldSize == 0)
					return;

				if (freeMemoryPercentage < 10) {
					OLogManager.instance().debug(this, "Low memory (%d%%): clearing %d cached records", freeMemoryPercentage, size());
					removeEldest(oldSize);
				} else {
					final int newSize = (int) (oldSize * 0.9f);
					removeEldest(oldSize - newSize);
					OLogManager.instance().debug(this, "Low memory (%d%%): reducing cached records number from %d to %d",
							freeMemoryPercentage, oldSize, newSize);
				}
			} catch (Exception e) {
				OLogManager.instance().error(this, "Error occurred during default cache cleanup", e);
			}
		}

	}
}
