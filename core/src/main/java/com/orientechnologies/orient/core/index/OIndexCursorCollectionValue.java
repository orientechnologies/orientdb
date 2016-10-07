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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of index cursor in case of collection of values which belongs to single key should be returned.
 * 
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/4/14
 */
public class OIndexCursorCollectionValue extends OIndexAbstractCursor implements OSizeable {
  private final Object              key;
  private Collection<OIdentifiable> collection;
  private Iterator<OIdentifiable>   iterator;

  public OIndexCursorCollectionValue(final Collection<OIdentifiable> collection, final Object key) {
    this.collection = collection;
    this.iterator = collection.iterator();
    this.key = key;
  }

  @Override
  public boolean hasNext() {
    if (iterator == null)
      return false;

    if (!iterator.hasNext()) {
      iterator = null;
      return false;
    }

    return true;
  }

  @Override
  public OIdentifiable next() {
    return iterator.next();
  }

  @Override
  public Map.Entry<Object, OIdentifiable> nextEntry() {
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

  @Override
  public int size() {
    return collection.size();
  }
}
