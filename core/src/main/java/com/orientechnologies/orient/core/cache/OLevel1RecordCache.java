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

import static com.orientechnologies.orient.core.metadata.OMetadata.CLUSTER_INDEX_NAME;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Level1 cache. it's one to one with record database instances.
 * 
 * @author Luca Garulli
 */
public class OLevel1RecordCache extends OAbstractRecordCache {
  private OLevel2RecordCache secondary = null;
  private String             CACHE_HIT;
  private String             CACHE_MISS;

  public OLevel1RecordCache(OCacheLevelOneLocator cacheLocator) {
    super(cacheLocator.threadLocalCache());
  }

  @Override
  public void startup() {
    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    secondary = db.getLevel2Cache();

    profilerPrefix = "db." + db.getName() + ".cache.level1.";
    profilerMetadataPrefix = "db.*.cache.level1.";

    CACHE_HIT = profilerPrefix + "cache.found";
    CACHE_MISS = profilerPrefix + "cache.notFound";

    excludedCluster = db.getClusterIdByName(CLUSTER_INDEX_NAME);

    super.startup();
    setEnable(OGlobalConfiguration.CACHE_LEVEL1_ENABLED.getValueAsBoolean());
  }

  /**
   * Pushes record to cache. Identifier of record used as access key
   * 
   * @param record
   *          record that should be cached
   */
  public void updateRecord(final ORecordInternal<?> record) {
    if (isEnabled() && record.getIdentity().getClusterId() != excludedCluster && record.getIdentity().isValid()
        && !record.getRecordVersion().isTombstone()) {
      underlying.lock(record.getIdentity());
      try {
        if (underlying.get(record.getIdentity()) != record)
          underlying.put(record);
      } finally {
        underlying.unlock(record.getIdentity());
      }
    }

    if (record.getIdentity().getClusterId() != excludedCluster)
      secondary.updateRecord(record);
  }

  /**
   * Looks up for record in cache by it's identifier. Optionally look up in secondary cache and update primary with found record
   * 
   * @param rid
   *          unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  public ORecordInternal<?> findRecord(final ORID rid) {
    if (!isEnabled()) {
      if (rid.getClusterId() != excludedCluster)
        return secondary.retrieveRecord(rid);
      else
        return null;
    }
    // DELEGATE TO THE 2nd LEVEL CACHE

    ORecordInternal<?> record;
    underlying.lock(rid);
    try {
      record = underlying.get(rid);

      if (record == null) {
        // DELEGATE TO THE 2nd LEVEL CACHE
        record = secondary.retrieveRecord(rid);

        if (record != null)
          // STORE IT LOCALLY
          underlying.put(record);
      }
    } finally {
      underlying.unlock(rid);
    }

    if (record != null)
      Orient.instance().getProfiler().updateCounter(CACHE_HIT, "Record found in Level1 Cache", 1L, "db.*.cache.level1.cache.found");
    else
      Orient.instance().getProfiler()
          .updateCounter(CACHE_MISS, "Record not found in Level1 Cache", 1L, "db.*.cache.level1.cache.notFound");

    return record;
  }

  /**
   * Removes record with specified identifier from both primary and secondary caches
   * 
   * @param rid
   *          unique identifier of record
   */
  public void deleteRecord(final ORID rid) {
    super.deleteRecord(rid);
    secondary.freeRecord(rid);
  }

  public void shutdown() {
    super.shutdown();
    secondary = null;
  }

  @Override
  public void clear() {
    moveRecordsToSecondaryCache();
    super.clear();
  }

  private void moveRecordsToSecondaryCache() {
    if (secondary == null)
      return;

    for (ORID rid : underlying.keys())
      secondary.updateRecord(underlying.get(rid));
  }

  /**
   * Invalidates the cache emptying all the records.
   */
  public void invalidate() {
    underlying.clear();
  }

  @Override
  public String toString() {
    return "DB level1 cache records = " + getSize() + ", maxSize= " + getMaxSize();
  }
}
