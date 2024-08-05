/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import java.util.HashSet;
import java.util.Set;

/**
 * Local cache. it's one to one with record database instances. It is needed to avoid cases when
 * several instances of the same record will be loaded by user from the same database.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OLocalRecordCache {
  protected ORecordCache underlying;
  protected String profilerPrefix = "noname";
  protected String profilerMetadataPrefix = "noname";
  protected int excludedCluster = -1;
  private String cacheHit;
  private String cacheMiss;

  public OLocalRecordCache() {
    underlying =
        Orient.instance()
            .getLocalRecordCache()
            .newInstance(OGlobalConfiguration.CACHE_LOCAL_IMPL.getValueAsString());
  }

  public void startup() {
    ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();

    profilerPrefix = "db." + db.getName() + ".cache.level1.";
    profilerMetadataPrefix = "db.*.cache.level1.";

    cacheHit = profilerPrefix + "cache.found";
    cacheMiss = profilerPrefix + "cache.notFound";
    underlying.startup();

    Orient.instance()
        .getProfiler()
        .registerHookValue(
            profilerPrefix + "current",
            "Number of entries in cache",
            METRIC_TYPE.SIZE,
            new OProfilerHookValue() {
              public Object getValue() {
                return getSize();
              }
            },
            profilerMetadataPrefix + "current");
  }

  /**
   * Pushes record to cache. Identifier of record used as access key
   *
   * @param record record that should be cached
   */
  public void updateRecord(final ORecord record) {
    if (record.getIdentity().getClusterId() != excludedCluster
        && record.getIdentity().isValid()
        && !record.isDirty()
        && !ORecordVersionHelper.isTombstone(record.getVersion())) {
      if (underlying.get(record.getIdentity()) != record) underlying.put(record);
    }
  }

  /**
   * Looks up for record in cache by it's identifier. Optionally look up in secondary cache and
   * update primary with found record
   *
   * @param rid unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  public ORecord findRecord(final ORID rid) {
    ORecord record;
    record = underlying.get(rid);

    if (record != null)
      Orient.instance()
          .getProfiler()
          .updateCounter(
              cacheHit, "Record found in Level1 Cache", 1L, "db.*.cache.level1.cache.found");
    else
      Orient.instance()
          .getProfiler()
          .updateCounter(
              cacheMiss,
              "Record not found in Level1 Cache",
              1L,
              "db.*.cache.level1.cache.notFound");

    return record;
  }

  /**
   * Removes record with specified identifier from both primary and secondary caches
   *
   * @param rid unique identifier of record
   */
  public void deleteRecord(final ORID rid) {
    underlying.remove(rid);
  }

  public void shutdown() {
    underlying.shutdown();

    if (Orient.instance().getProfiler() != null) {
      Orient.instance().getProfiler().unregisterHookValue(profilerPrefix + "enabled");
      Orient.instance().getProfiler().unregisterHookValue(profilerPrefix + "current");
      Orient.instance().getProfiler().unregisterHookValue(profilerPrefix + "max");
    }
  }

  public void clear() {
    underlying.clear();
  }

  /** Invalidates the cache emptying all the records. */
  public void invalidate() {
    underlying.clear();
  }

  @Override
  public String toString() {
    return "DB level cache records = " + getSize();
  }

  /**
   * Tell whether cache is enabled
   *
   * @return {@code true} if cache enabled at call time, otherwise - {@code false}
   */
  public boolean isEnabled() {
    return underlying.isEnabled();
  }

  /**
   * Switch cache state between enabled and disabled
   *
   * @param enable pass {@code true} to enable, otherwise - {@code false}
   */
  public void setEnable(final boolean enable) {
    if (enable) underlying.enable();
    else underlying.disable();
  }

  /**
   * Remove record with specified identifier
   *
   * @param rid unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  public ORecord freeRecord(final ORID rid) {
    return underlying.remove(rid);
  }

  /**
   * Remove all records belonging to specified cluster
   *
   * @param cid identifier of cluster
   */
  public void freeCluster(final int cid) {
    final Set<ORID> toRemove = new HashSet<ORID>(underlying.size() / 2);

    final Set<ORID> keys = new HashSet<ORID>(underlying.keys());
    for (final ORID id : keys) if (id.getClusterId() == cid) toRemove.add(id);

    for (final ORID ridToRemove : toRemove) underlying.remove(ridToRemove);
  }

  /**
   * Total number of cached entries
   *
   * @return non-negative integer
   */
  public int getSize() {
    return underlying.size();
  }

  public void clearRecords() {
    underlying.clearRecords();
  }
}
