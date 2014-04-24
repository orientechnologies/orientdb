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
 * {@link #next(int)} returns <code>null</code>.
 * 
 * Cursor is created as result of index query method such as
 * {@link com.orientechnologies.orient.core.index.OIndex#iterateEntriesBetween(Object, boolean, Object, boolean, boolean)} cursor
 * instance can not be used at several threads simultaneously.
 * 
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 4/4/14
 */
public interface OIndexCursor {
  /**
   * Returns next element in subset of index data which should be iterated by given cursor.
   * 
   * @param prefetchSize
   *          Size of data which should be prefetched from index into heap and then used in next iteration. It allows to speed up
   *          index queries. The actual size of prefetched data may be different and depends on real implementation.
   * @return next element in subset of index data which should be iterated by given cursor or <code>null</code> if all data are
   *         iterated.
   */
  Map.Entry<Object, OIdentifiable> next(int prefetchSize);

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
   * Implementation of index cursor in case of only single entree should be returned.
   */
  final class OIndexCursorSingleValue extends OIndexAbstractCursor {
    private OIdentifiable identifiable;
    private final Object  key;

    public OIndexCursorSingleValue(OIdentifiable identifiable, Object key) {
      this.identifiable = identifiable;
      this.key = key;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> next(int prefetchSize) {
      if (identifiable == null)
        return null;

      final OIdentifiable value = identifiable;
      identifiable = null;

      return new Map.Entry<Object, OIdentifiable>() {

        @Override
        public Object getKey() {
          return key;
        }

        @Override
        public OIdentifiable getValue() {
          return value;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }
  }

  /**
   * Implementation of index cursor in case of collection of values which belongs to single key should be returned.
   */
  final class OIndexCursorCollectionValue extends OIndexAbstractCursor {
    private Iterator<OIdentifiable> iterator;
    private final Object            key;

    public OIndexCursorCollectionValue(Iterator<OIdentifiable> iterator, Object key) {
      this.iterator = iterator;
      this.key = key;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> next(int prefetchSize) {
      if (iterator == null)
        return null;

      if (!iterator.hasNext()) {
        iterator = null;
        return null;
      }

      final OIdentifiable value = iterator.next();
      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return key;
        }

        @Override
        public OIdentifiable getValue() {
          return value;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }
  }

}
