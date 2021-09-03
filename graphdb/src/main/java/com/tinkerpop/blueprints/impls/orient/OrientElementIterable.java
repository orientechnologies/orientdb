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

package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import java.util.Collections;
import java.util.Iterator;

/** @author Marko A. Rodriguez (http://markorodriguez.com) */
public class OrientElementIterable<T extends Element> implements CloseableIterable<T> {

  private final Iterable<?> iterable;
  protected final OrientBaseGraph graph;

  public OrientElementIterable(final OrientBaseGraph graph, final Iterable<?> iterable) {
    this.graph = graph;
    this.iterable = iterable;
  }

  @SuppressWarnings("unchecked")
  public Iterator<T> iterator() {
    if (iterable == null) return Collections.EMPTY_LIST.iterator();

    return new OrientElementIterator<T>(this.graph, iterable.iterator());
  }

  public void close() {}
}
