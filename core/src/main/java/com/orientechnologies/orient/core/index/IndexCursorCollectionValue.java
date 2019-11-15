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

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Implementation of index cursor in case of collection of values which belongs to single key should be returned.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/4/14
 */
public final class IndexCursorCollectionValue implements OSizeable, IndexCursor {
  private final Object                    key;
  private final Collection<OIdentifiable> collection;
  private       Iterator<OIdentifiable>   iterator;

  public IndexCursorCollectionValue(final Collection<OIdentifiable> collection, final Object key) {
    this.collection = collection;
    this.iterator = collection.iterator();
    this.key = key;
  }

  @Override
  public int size() {
    return collection.size();
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
    if (iterator == null) {
      return false;
    }

    if (!iterator.hasNext()) {
      iterator = null;
      return false;
    }

    action.accept(new ORawPair<>(key, iterator.next().getIdentity()));
    return true;
  }

  @Override
  public Spliterator<ORawPair<Object, ORID>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return collection.size();
  }

  @Override
  public int characteristics() {
    return NONNULL | SIZED | ORDERED;
  }
}
