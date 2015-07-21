/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Local cache. it's one to one with record database instances. It is needed to avoid cases when several instances of the same
 * record will be loaded by user from the same database.
 * 
 * @author Luca Garulli
 */
public class OLocalRecordCache extends OAbstractRecordCache {
  private String CACHE_HIT;
  private String CACHE_MISS;

  public OLocalRecordCache(OCacheLevelOneLocator cacheLocator) {
    super(cacheLocator.threadLocalCache());
  }

  @Override
  public void startup() {
    ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();

    profilerPrefix = "db." + db.getName() + ".cache.level1.";
    profilerMetadataPrefix = "db.*.cache.level1.";

    CACHE_HIT = profilerPrefix + "cache.found";
    CACHE_MISS = profilerPrefix + "cache.notFound";

    super.startup();
  }

  /**
   * Pushes record to cache. Identifier of record used as access key
   * 
   * @param record
   *          record that should be cached
   */
  public void updateRecord(final ORecord record) {
    if (record.getIdentity().getClusterId() != excludedCluster && record.getIdentity().isValid() && !record.isDirty()
        && !record.getRecordVersion().isTombstone()) {
      if (underlying.get(record.getIdentity()) != record)
        underlying.put(record);
    }
  }

  /**
   * Looks up for record in cache by it's identifier. Optionally look up in secondary cache and update primary with found record
   * 
   * @param rid
   *          unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  public ORecord findRecord(final ORID rid) {
    ORecord record;
    record = underlying.get(rid);

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
  }

  public void shutdown() {
    super.shutdown();
  }

  @Override
  public void clear() {
    super.clear();
  }

  /**
   * Invalidates the cache emptying all the records.
   */
  public void invalidate() {
    underlying.clear();
  }

  @Override
  public String toString() {
    return "DB level cache records = " + getSize();
  }
}
