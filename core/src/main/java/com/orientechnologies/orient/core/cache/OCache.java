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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.util.Collection;

/**
 * Generic cache interface that should be implemented in order to plug-in custom cache.
 * Also implementing class has to have public one-arg constructor to set cache limit.
 * For example, next class can be safely used as plug-in cache:
 * <pre>
 *   public class CustomCache implements OCache {
 *     public CustomCache(int initialLimit) {
 *       // some actions to do basic initialization of cache instance
 *       ...
 *     }
 *
 *     //implementation of interface
 *     ...
 *   }
 * </pre>
 * As reference implementation used {@link ODefaultCache}
 *
 * @author Maxim Fedorov
 */
public interface OCache {
  /**
   * All operations running at cache initialization stage
   */
  void startup();

  /**
   * All operations running at cache destruction stage
   */
  void shutdown();

  /**
   * Tell whether cache is enabled
   *
   * @return {@code true} if cache enabled at call time, otherwise - {@code false}
   */
  boolean isEnabled();

  /**
   * Enable cache
   *
   * @return {@code true} - if enabled, {@code false} - otherwise (already enabled)
   */
  boolean enable();

  /**
   * Disable cache. None of record management methods will cause effect on cache in disabled state.
   * Only cache info methods available at that state.
   *
   * @return {@code true} - if disabled, {@code false} - otherwise (already disabled)
   */
  boolean disable();

  /**
   * Look up for record in cache by it's identifier
   *
   * @param id unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  ORecordInternal<?> get(ORID id);

  /**
   * Push record to cache. Identifier of record used as access key
   *
   * @param record record that should be cached
   * @return previous version of record
   */
  ORecordInternal<?> put(ORecordInternal<?> record);

  /**
   * Remove record with specified identifier
   *
   * @param id unique identifier of record
   * @return record stored in cache if any, otherwise - {@code null}
   */
  ORecordInternal<?> remove(ORID id);

  /**
   * Remove all records from cache
   */
  void clear();

  /**
   * Total number of stored records
   *
   * @return non-negative number
   */
  int size();

  /**
   * Maximum number of items cache should keep
   *
   * @return non-negative number
   */
  int limit();

  /**
   * Keys of all stored in cache records
   *
   * @return keys of records
   */
  Collection<ORID> keys();

  /**
   * Lock the item with given id, even if item does not exist all read/update
   * operations for given item should be locked.
   * 
   * @param id Item to lock.
   */
  void lock(ORID id);

  /**
   * Unlock item.
   *
   * @param id item to unlock;
   */
  void unlock(ORID id);
}
