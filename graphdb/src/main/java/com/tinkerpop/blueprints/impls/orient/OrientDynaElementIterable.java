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
import java.util.Iterator;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com) */
public class OrientDynaElementIterable implements CloseableIterable<Object> {

  private final Iterator<?> iterator;
  private final OrientBaseGraph graph;

  public OrientDynaElementIterable(final OrientBaseGraph graph, final Iterable<?> iterable) {
    this.graph = graph;
    this.iterator = iterable.iterator();
  }

  public OrientDynaElementIterable(final OrientBaseGraph graph, final Iterator<?> iterator) {
    this.graph = graph;
    this.iterator = iterator;
  }

  public Iterator<Object> iterator() {
    return new OrientDynaElementIterator(this.graph, iterator);
  }

  public void close() {}
}
