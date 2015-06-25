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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.lock.OAdaptiveLock;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.query.OResultSet;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Command cache implementation that uses Weak pointers to avoid overloading Java Heap.
 * 
 * @author Luca Garulli
 */
public class OUnboundedWeakCommandCache implements OCommandCache {
  private static class OCachedResult {
    Object      result;
    Set<String> involvedClusters;

    public OCachedResult(final Object result, final Set<String> involvedClusters) {
      this.involvedClusters = involvedClusters;
      this.result = result;
    }

    public void clear() {
      result = null;
      involvedClusters = null;
    }
  }

  private class OCommandCacheImpl extends WeakHashMap<String, WeakReference<OCachedResult>> {
  }

  public enum STRATEGY {
    INVALIDATE_ALL, PER_CLUSTER
  }

  private final String      databaseName;
  private volatile boolean  enable           = OGlobalConfiguration.COMMAND_CACHE_ENABLED.getValueAsBoolean();
  private OCommandCacheImpl cache            = new OCommandCacheImpl();
  private int               minExecutionTime = OGlobalConfiguration.COMMAND_CACHE_MIN_EXECUTION_TIME.getValueAsInteger();
  private int               maxResultsetSize = OGlobalConfiguration.COMMAND_CACHE_MAX_RESULSET_SIZE.getValueAsInteger();

  private STRATEGY          evictStrategy    = STRATEGY.valueOf(OGlobalConfiguration.COMMAND_CACHE_EVICT_STRATEGY
                                                 .getValueAsString());

  private OAdaptiveLock     lock             = new OAdaptiveLock(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());

  public OUnboundedWeakCommandCache(final String iDatabaseName) {
    databaseName = iDatabaseName;
  }

  @Override
  public void startup() {
  }

  @Override
  public void shutdown() {
    lock.lock();
    try {

      cache = new OCommandCacheImpl();

    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isEnabled() {
    return enable;
  }

  @Override
  public OUnboundedWeakCommandCache enable() {
    enable = true;
    return this;
  }

  @Override
  public OUnboundedWeakCommandCache disable() {
    enable = false;
    return this;
  }

  @Override
  public Object get(final OSecurityUser iUser, final String queryText, final int iLimit) {
    if (!enable)
      return null;

    final String key = getKey(iUser, queryText, iLimit);

    Object result;

    lock.lock();
    try {
      final WeakReference<OCachedResult> value = cache.get(key);

      result = get(value);

      if (result != null)
        // SERIALIZE ALL THE RECORDS IN LOCK TO AVOID CONCURRENT ACCESS. ONCE SERIALIZED CAN ARE THREAD-SAFE
        if (result instanceof ORecord)
          ((ORecord) result).toStream();
        else if (OMultiValue.isMultiValue(result)) {
          for (Object rc : OMultiValue.getMultiValueIterable(result)) {
            if (rc != null && rc instanceof ORecord) {
              ((ORecord) rc).toStream();
            }
          }
        }

    } finally {
      lock.unlock();
    }

    final OProfilerMBean profiler = Orient.instance().getProfiler();
    if (profiler.isRecording()) {
      // UPDATE PROFILER
      if (result != null) {
        profiler.updateCounter(profiler.getDatabaseMetric(databaseName, "queryCache.hit"), "Results returned by Query Cache", +1);

      } else {
        profiler.updateCounter(profiler.getDatabaseMetric(databaseName, "queryCache.miss"), "Results not returned by Query Cache",
            +1);
      }
    }
    return result;
  }

  @Override
  public void put(final OSecurityUser iUser, final String queryText, final Object iResult, final int iLimit,
      Set<String> iInvolvedClusters, final long iExecutionTime) {
    if (queryText == null || iResult == null || iInvolvedClusters == null || iInvolvedClusters.isEmpty())
      // SKIP IT
      return;

    if (!enable)
      return;

    if (iExecutionTime < minExecutionTime)
      // TOO FAST: AVOIDING CACHING IT
      return;

    if (iResult instanceof OResultSet && ((OResultSet) iResult).size() > maxResultsetSize)
      // TOO BIG RESULTSET, SKIP IT
      return;

    if (evictStrategy != STRATEGY.PER_CLUSTER)
      iInvolvedClusters = null;

    final String key = getKey(iUser, queryText, iLimit);
    final WeakReference<OCachedResult> value = new WeakReference<OCachedResult>(new OCachedResult(iResult, iInvolvedClusters));

    lock.lock();
    try {
      cache.put(key, value);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void remove(final OSecurityUser iUser, final String queryText, final int iLimit) {
    if (!enable)
      return;

    final String key = getKey(iUser, queryText, iLimit);

    lock.lock();
    try {
      cache.remove(key);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public OUnboundedWeakCommandCache clear() {
    lock.lock();
    try {

      cache = new OCommandCacheImpl();

    } finally {
      lock.unlock();
    }
    return this;
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return cache.size();

    } finally {
      lock.unlock();
    }
  }

  @Override
  public void invalidateResultsOfCluster(final String iCluster) {
    if (!enable)
      return;

    if (evictStrategy == STRATEGY.INVALIDATE_ALL) {
      clear();
      return;
    }

    lock.lock();
    try {
      for (Iterator<Map.Entry<String, WeakReference<OCachedResult>>> it = cache.entrySet().iterator(); it.hasNext();) {
        final WeakReference<OCachedResult> pointer = it.next().getValue();
        if (pointer != null) {
          final OCachedResult cached = pointer.get();
          if (cached != null) {
            if (cached.involvedClusters.contains(iCluster)) {
              cached.clear();
              it.remove();
            }
          }
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public int getMinExecutionTime() {
    return minExecutionTime;
  }

  public OUnboundedWeakCommandCache setMinExecutionTime(final int minExecutionTime) {
    this.minExecutionTime = minExecutionTime;
    return this;
  }

  public int getMaxResultsetSize() {
    return maxResultsetSize;
  }

  public OUnboundedWeakCommandCache setMaxResultsetSize(final int maxResultsetSize) {
    this.maxResultsetSize = maxResultsetSize;
    return this;
  }

  public STRATEGY getEvictStrategy() {
    return evictStrategy;
  }

  public OUnboundedWeakCommandCache setEvictStrategy(final STRATEGY evictStrategy) {
    this.evictStrategy = evictStrategy;
    return this;
  }

  protected String getKey(final OSecurityUser iUser, final String queryText, final int iLimit) {
    if (iUser == null)
      return "<nouser>." + queryText + "." + iLimit;

    return iUser + "." + queryText + "." + iLimit;
  }

  protected Object get(final WeakReference<OCachedResult> value) {
    if (value != null) {
      final OCachedResult cached = value.get();
      if (cached != null)
        return cached.result;
    }
    return null;
  }
}
