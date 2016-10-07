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

import com.orientechnologies.orient.core.metadata.security.OSecurityUser;

import java.util.Set;

/**
 * Generic query cache interface.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OCommandCache {
  /**
   * All operations running at cache initialization stage.
   */
  void startup();

  /**
   * All operations running at cache destruction stage.
   */
  void shutdown();

  /**
   * Tells whether cache is enabled.
   *
   * @return {@code true} if cache enabled at call time, otherwise - {@code false}
   */
  boolean isEnabled();

  /**
   * Enables cache.
   */
  OCommandCache enable();

  /**
   * Disables cache. None of query methods will cause effect on cache in disabled state. Only cache info methods available at that
   * state.
   */
  OCommandCache disable();

  /**
   * Looks up for query result in cache.
   */
  Object get(OSecurityUser iUser, String queryText, int iLimit);

  /**
   * Pushes record to cache. Identifier of record used as access key
   */
  void put(OSecurityUser iUser, String queryText, Object iResult, int iLimit, Set<String> iInvolvedClusters, long iExecutionTime);

  /**
   * Removes result of query.
   **/
  void remove(OSecurityUser iUser, String queryText, int iLimit);

  /**
   * Remove all results from the cache.
   */
  OCommandCache clear();

  /**
   * Total number of stored queries.
   * 
   * @return non-negative number
   */
  int size();

  /**
   * Invalidates results of given cluster.
   *
   * @param iCluster
   *          Cluster name
   */
  void invalidateResultsOfCluster(final String iCluster);

  int getMaxResultsetSize();

  STRATEGY getEvictStrategy();

  public enum STRATEGY {
    INVALIDATE_ALL, PER_CLUSTER
  }
}
