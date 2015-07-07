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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.query.OResultSet;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Command cache implementation that uses Soft references to avoid overloading Java Heap.
 * 
 * @author Luca Garulli
 */
public class OCommandCacheSoftRefs implements OCommandCache {
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

  private class OCommandCacheImplRefs extends OSoftRefsHashMap<String, OCachedResult> {
  }

  public enum STRATEGY {
    INVALIDATE_ALL, PER_CLUSTER
  }

  private final String          databaseName;
  private Set<String>           clusters         = new HashSet<String>();
  private volatile boolean      enable           = OGlobalConfiguration.COMMAND_CACHE_ENABLED.getValueAsBoolean();
  private OCommandCacheImplRefs cache            = new OCommandCacheImplRefs();
  private int                   minExecutionTime = OGlobalConfiguration.COMMAND_CACHE_MIN_EXECUTION_TIME.getValueAsInteger();
  private int                   maxResultsetSize = OGlobalConfiguration.COMMAND_CACHE_MAX_RESULSET_SIZE.getValueAsInteger();

  private STRATEGY              evictStrategy    = STRATEGY.valueOf(OGlobalConfiguration.COMMAND_CACHE_EVICT_STRATEGY
                                                     .getValueAsString());

  public OCommandCacheSoftRefs(final String iDatabaseName) {
    databaseName = iDatabaseName;
  }

  @Override
  public void startup() {
  }

  @Override
  public void shutdown() {
    clear();
  }

  @Override
  public boolean isEnabled() {
    return enable;
  }

  @Override
  public OCommandCacheSoftRefs enable() {
    enable = true;
    return this;
  }

  @Override
  public OCommandCacheSoftRefs disable() {
    enable = false;
    synchronized (this) {
      clusters.clear();
      cache.clear();
    }
    return this;
  }

  @Override
  public Object get(final OSecurityUser iUser, final String queryText, final int iLimit) {
    if (!enable)
      return null;

    OCachedResult result;

    synchronized (this) {
      final String key = getKey(iUser, queryText, iLimit);

      result = cache.get(key);

      if (result != null) {
        // SERIALIZE ALL THE RECORDS IN LOCK TO AVOID CONCURRENT ACCESS. ONCE SERIALIZED CAN ARE THREAD-SAFE
        int resultsetSize = 1;

        if (result.result instanceof ORecord)
          ((ORecord) result.result).toStream();
        else if (OMultiValue.isMultiValue(result.result)) {
          resultsetSize = OMultiValue.getSize(result.result);
          for (Object rc : OMultiValue.getMultiValueIterable(result.result)) {
            if (rc != null && rc instanceof ORecord) {
              ((ORecord) rc).toStream();
            }
          }
        }

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "Reused cached resultset size=%d", resultsetSize);
      }
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

    return result != null ? result.result : null;
  }

  @Override
  public void put(final OSecurityUser iUser, final String queryText, final Object iResult, final int iLimit,
      Set<String> iInvolvedClusters, final long iExecutionTime) {
    if (queryText == null || iResult == null)
      // SKIP IT
      return;

    if (!enable)
      return;

    if (iExecutionTime < minExecutionTime)
      // TOO FAST: AVOIDING CACHING IT
      return;

    int resultsetSize = 1;
    if (iResult instanceof OResultSet) {
      resultsetSize = ((OResultSet) iResult).size();

      if (resultsetSize > maxResultsetSize)
        // TOO BIG RESULTSET, SKIP IT
        return;
    }

    if (evictStrategy != STRATEGY.PER_CLUSTER)
      iInvolvedClusters = null;

    synchronized (this) {
      final String key = getKey(iUser, queryText, iLimit);
      final OCachedResult value = new OCachedResult(iResult, iInvolvedClusters);

      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Storing resultset in cache size=%d", resultsetSize);

      cache.put(key, value);

      clusters.addAll(iInvolvedClusters);
    }
  }

  @Override
  public void remove(final OSecurityUser iUser, final String queryText, final int iLimit) {
    if (!enable)
      return;

    synchronized (this) {
      final String key = getKey(iUser, queryText, iLimit);
      cache.remove(key);
    }
  }

  @Override
  public OCommandCacheSoftRefs clear() {
    synchronized (this) {
      cache = new OCommandCacheImplRefs();
      clusters.clear();
    }
    return this;
  }

  @Override
  public int size() {
    synchronized (this) {
      return cache.size();
    }
  }

  @Override
  public void invalidateResultsOfCluster(final String iCluster) {
    if (!enable)
      return;

    synchronized (this) {
      if (cache.size() == 0)
        return;

      if (evictStrategy == STRATEGY.INVALIDATE_ALL) {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "Invalidate all cached results (%d)  ", size());

        clear();
        return;
      }

      if (!clusters.remove(iCluster)) {
        // NOT CONTAINED, AVOID COSTLY BROWSING OF RESULTS
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "No results found for '%s'", iCluster);
        return;
      }

      int evicted = 0;
      for (Iterator<Map.Entry<String, OCachedResult>> it = cache.entrySet().iterator(); it.hasNext();) {
        final OCachedResult cached = it.next().getValue();
        if (cached != null) {
          if (cached.involvedClusters == null || cached.involvedClusters.isEmpty() || cached.involvedClusters.contains(iCluster)) {
            cached.clear();
            it.remove();
          }
        }
      }

      if (evicted > 0 && OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Invalidate %d cached results associated to the cluster '%s'", evicted, iCluster);
    }
  }

  public int getMinExecutionTime() {
    return minExecutionTime;
  }

  public OCommandCacheSoftRefs setMinExecutionTime(final int minExecutionTime) {
    this.minExecutionTime = minExecutionTime;
    return this;
  }

  @Override
  public int getMaxResultsetSize() {
    return maxResultsetSize;
  }

  public OCommandCacheSoftRefs setMaxResultsetSize(final int maxResultsetSize) {
    this.maxResultsetSize = maxResultsetSize;
    return this;
  }

  @Override
  public STRATEGY getEvictStrategy() {
    return evictStrategy;
  }

  public OCommandCacheSoftRefs setEvictStrategy(final STRATEGY evictStrategy) {
    this.evictStrategy = evictStrategy;
    return this;
  }

  protected String getKey(final OSecurityUser iUser, final String queryText, final int iLimit) {
    if (iUser == null)
      return "<nouser>." + queryText + "." + iLimit;

    return iUser + "." + queryText + "." + iLimit;
  }
}
