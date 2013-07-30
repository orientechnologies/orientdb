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
package com.orientechnologies.orient.core.cache;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CACHE_LEVEL2_STRATEGY;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Second level cache. It's shared among database instances with the same URL.
 * 
 * @author Luca Garulli
 * @author Sylvain Spinelli
 */
public class OLevel2RecordCache extends OAbstractRecordCache {
  private final String CACHE_HIT;
  private final String CACHE_MISS;
  private STRATEGY     strategy;

  public enum STRATEGY {
    POP_RECORD, COPY_RECORD
  }

  public OLevel2RecordCache(final OStorage storage, OCacheLevelTwoLocator cacheLocator) {
    super(cacheLocator.primaryCache(storage.getName()));

    profilerPrefix = "db." + storage.getName() + ".cache.level2.";
    profilerMetadataPrefix = "db.*.cache.level2.";

    CACHE_HIT = profilerPrefix + "cache.found";
    CACHE_MISS = profilerPrefix + "cache.notFound";

    strategy = STRATEGY.values()[(CACHE_LEVEL2_STRATEGY.getValueAsInteger())];
  }

  @Override
  public void startup() {
    super.startup();
    setEnable(OGlobalConfiguration.CACHE_LEVEL2_ENABLED.getValueAsBoolean());
  }

  /**
   * Push record to cache. Identifier of record used as access key
   * 
   * @param fresh
   *          new record that should be cached
   */
  public void updateRecord(final ORecordInternal<?> fresh) {
    if (!isEnabled() || fresh == null || fresh.isDirty() || fresh.getIdentity().isNew() || !fresh.getIdentity().isValid()
        || fresh.getIdentity().getClusterId() == excludedCluster || fresh.getRecordVersion().isTombstone())
      return;

    if (fresh.isPinned() == null || fresh.isPinned()) {
      underlying.lock(fresh.getIdentity());
      try {
        final ORecordInternal<?> current = underlying.get(fresh.getIdentity());
        if (current != null && current.getRecordVersion().compareTo(fresh.getRecordVersion()) >= 0)
          return;

        if (ODatabaseRecordThreadLocal.INSTANCE.isDefined() && !ODatabaseRecordThreadLocal.INSTANCE.get().isClosed())
          // CACHE A COPY
          underlying.put((ORecordInternal<?>) fresh.flatCopy());
        else {
          // CACHE THE DETACHED RECORD
          fresh.detach();
          underlying.put(fresh);
        }
      } finally {
        underlying.unlock(fresh.getIdentity());
      }
    } else
      underlying.remove(fresh.getIdentity());
  }

  /**
   * Retrieve the record if any following the supported strategies:<br>
   * 0 = If found remove it (pop): the client (database instances) will push it back when finished or on close.<br>
   * 1 = Return the instance but keep a copy in 2-level cache; this could help highly-concurrent environment.
   * 
   * @param iRID
   *          record identity
   * @return record if exists in cache, {@code null} otherwise
   */
  protected ORecordInternal<?> retrieveRecord(final ORID iRID) {
    if (!isEnabled() || iRID.getClusterId() == excludedCluster)
      return null;

    ORecordInternal<?> record;
    underlying.lock(iRID);
    try {
      record = underlying.remove(iRID);

      if (record == null || record.isDirty()) {
        Orient.instance().getProfiler()
            .updateCounter(CACHE_MISS, "Record not found in Level2 Cache", +1, "db.*.cache.level2.cache.notFound");
        return null;
      }

      if (strategy == STRATEGY.COPY_RECORD) {
        final ORecordInternal<?> resident = OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean() ? (ORecordInternal<?>) record
            .flatCopy() : record;
        // PUT BACK A COPY OR ThE ORIGINAL IF NOT MULTI-THREADS (THIS UPDATE ALSO THE LRU)
        underlying.put(resident);
      }

    } finally {
      underlying.unlock(iRID);
    }

    Orient.instance().getProfiler().updateCounter(CACHE_HIT, "Record found in Level2 Cache", +1, "db.*.cache.level2.cache.found");
    return record;
  }

  public void setStrategy(final STRATEGY newStrategy) {
    strategy = newStrategy;
  }

  @Override
  public String toString() {
    return "STORAGE level2 cache records = " + getSize() + ", maxSize = " + getMaxSize();
  }
}
