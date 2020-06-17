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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/24/14
 */
public abstract class OIndexAbstractCursor implements OIndexCursor {
  protected int prefetchSize = -1;
  private Map.Entry<Object, OIdentifiable> nextEntry;
  private boolean firstTime = true;

  @Override
  public Set<OIdentifiable> toValues() {
    final HashSet<OIdentifiable> result = new HashSet<OIdentifiable>();
    Map.Entry<Object, OIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry.getValue());
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public Set<Map.Entry<Object, OIdentifiable>> toEntries() {
    final HashSet<Map.Entry<Object, OIdentifiable>> result =
        new HashSet<Map.Entry<Object, OIdentifiable>>();

    Map.Entry<Object, OIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry);
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public Set<Object> toKeys() {
    final HashSet<Object> result = new HashSet<Object>();

    Map.Entry<Object, OIdentifiable> entry = nextEntry();

    while (entry != null) {
      result.add(entry.getKey());
      entry = nextEntry();
    }

    return result;
  }

  @Override
  public boolean hasNext() {
    if (firstTime) {
      nextEntry = nextEntry();
      firstTime = false;
    }

    return nextEntry != null;
  }

  @Override
  public OIdentifiable next() {
    if (!hasNext()) throw new NoSuchElementException();

    final Map.Entry<Object, OIdentifiable> result = nextEntry;
    nextEntry = nextEntry();

    return result.getValue();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }

  public int getPrefetchSize() {
    return prefetchSize;
  }

  public void setPrefetchSize(final int prefetchSize) {
    this.prefetchSize = prefetchSize;
  }
}
