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

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Implementation of index cursor in case of only single entree should be returned.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/4/14
 */
public class IndexCursorSingleValue implements OSizeable, IndexCursor {
  private final Object        key;
  private       OIdentifiable identifiable;
  private       boolean       empty = false;

  public IndexCursorSingleValue(OIdentifiable identifiable, Object key) {
    this.identifiable = identifiable;
    this.key = key;
    if (this.identifiable == null) {
      empty = true;
    }
  }

  @Override
  public int size() {
    return empty ? 0 : 1;
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
    if (identifiable == null) {
      return false;
    }

    action.accept(new ORawPair<>(key, identifiable.getIdentity()));
    identifiable = null;

    return true;
  }

  @Override
  public Spliterator<ORawPair<Object, ORID>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return 1;
  }

  @Override
  public int characteristics() {
    return NONNULL | SIZED | ORDERED;
  }
}
