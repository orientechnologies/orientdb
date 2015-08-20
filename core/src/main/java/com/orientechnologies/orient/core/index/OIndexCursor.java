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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Presentation of OrientDB index cursor for point and range queries. Cursor may iterate by several elements even if you do point
 * query (query by single key). It is possible if you use not unique index.
 * 
 * Contract of cursor is simple it iterates in some subset of index data till it reaches it's borders in such case
 * {@link #nextEntry()} returns <code>null</code>.
 * 
 * Cursor is created as result of index query method such as
 * {@link com.orientechnologies.orient.core.index.OIndex#iterateEntriesBetween(Object, boolean, Object, boolean, boolean)} cursor
 * instance cannot be used at several threads simultaneously.
 * 
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 4/4/14
 */
public interface OIndexCursor extends Iterator<OIdentifiable> {
  /**
   * Returns nextEntry element in subset of index data which should be iterated by given cursor.
   * 
   * @return nextEntry element in subset of index data which should be iterated by given cursor or <code>null</code> if all data are
   *         iterated.
   */
  Map.Entry<Object, OIdentifiable> nextEntry();

  /**
   * Accumulates and returns all values of index inside of data subset of cursor.
   * 
   * @return all values of index inside of data subset of cursor.
   */
  Set<OIdentifiable> toValues();

  /**
   * Accumulates and returns all entries of index inside of data subset of cursor.
   * 
   * @return all entries of index inside of data subset of cursor.
   */
  Set<Map.Entry<Object, OIdentifiable>> toEntries();

  /**
   * Accumulates and returns all keys of index inside of data subset of cursor.
   * 
   * @return all keys of index inside of data subset of cursor.
   */
  Set<Object> toKeys();

  /**
   * Set number of records to fetch for the next call to next() or nextEntry().
   * 
   * @param prefetchSize
   *          Number of records to prefetch. -1 = no prefetch
   */
  public void setPrefetchSize(int prefetchSize);
}
