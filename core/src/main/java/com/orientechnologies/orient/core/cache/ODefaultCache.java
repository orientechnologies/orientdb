/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.collection.OLimitedMap;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Default implementation of generic {@link OCache} interface that uses plain {@link LinkedHashMap} to store records
 * 
 * @author Maxim Fedorov
 */
public class ODefaultCache extends OAbstractMapCache<ODefaultCache.OLinkedHashMapCache> {
  private static final int           DEFAULT_LIMIT = 1000;

  protected final int                limit;
  protected OMemoryWatchDog.Listener lowMemoryListener;

  public ODefaultCache(final String iName, final int initialLimit) {
    super(new OLinkedHashMapCache(initialLimit > 0 ? initialLimit : DEFAULT_LIMIT, 0.75f, initialLimit));
    limit = initialLimit;
  }

  @Override
  public void startup() {
    lowMemoryListener = Orient.instance().getMemoryWatchDog().addListener(new OLowMemoryListener());
    super.startup();
  }

  @Override
  public void shutdown() {
    Orient.instance().getMemoryWatchDog().removeListener(lowMemoryListener);
    super.shutdown();
  }

  private void removeEldest(final int threshold) {
    lock.acquireExclusiveLock();
    try {
      cache.removeEldest(threshold);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public int limit() {
    return limit;
  }

  @Override
  public ORecordInternal<?> get(final ORID id) {
    if (!isEnabled())
      return null;

    lock.acquireExclusiveLock();
    try {
      return cache.get(id);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public ORecordInternal<?> put(final ORecordInternal<?> record) {
    if (!isEnabled())
      return null;

    lock.acquireExclusiveLock();
    try {
      return cache.put(record.getIdentity(), record);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  @Override
  public ORecordInternal<?> remove(final ORID id) {
    if (!isEnabled())
      return null;

    lock.acquireExclusiveLock();
    try {
      return cache.remove(id);
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Implementation of {@link LinkedHashMap} that will remove eldest entries if size limit will be exceeded.
   * 
   * @author Luca Garulli
   */
  @SuppressWarnings("serial")
  static final class OLinkedHashMapCache extends OLimitedMap<ORID, ORecordInternal<?>> {
    public OLinkedHashMapCache(final int initialCapacity, final float loadFactor, final int limit) {
      super(initialCapacity, loadFactor, limit);
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
    public void lowMemory(final long freeMemory, final long freeMemoryPercentage) {
      try {
        final int oldSize = size();
        if (oldSize == 0)
          return;

        if (freeMemoryPercentage < 10) {
          OLogManager.instance().warn(this, "Low heap memory (%d%%): clearing %d cached records", freeMemoryPercentage, size());
          removeEldest(oldSize);
        } else {
          final int newSize = (int) (oldSize * 0.9f);
          removeEldest(oldSize - newSize);
          OLogManager.instance().warn(this, "Low heap memory (%d%%): reducing cached records number from %d to %d",
              freeMemoryPercentage, oldSize, newSize);
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error occurred during default cache cleanup", e);
      }
    }
  }
}
