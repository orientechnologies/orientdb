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
import com.orientechnologies.orient.core.id.ORID;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 */
public class CompositeIndexCursor implements IndexCursor {
  private Collection<IndexCursor> cursors;
  private Iterator<IndexCursor>   cursorIterator;

  private final Iterator<ORawPair<Object, ORID>> entriesIterator;

  public CompositeIndexCursor(Collection<IndexCursor> cursors) {
    entriesIterator = cursors.stream().flatMap((cursor) -> StreamSupport.stream(cursor, false)).iterator();
  }

  @Override
  public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
    if (entriesIterator.hasNext()) {
      action.accept(entriesIterator.next());
      return true;
    }

    return false;
  }

  @Override
  public Spliterator<ORawPair<Object, ORID>> trySplit() {
    return null;
  }

  @Override
  public long estimateSize() {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics() {
    return NONNULL | ORDERED;
  }
}
