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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import java.util.Iterator;

/** @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com) */
class OrientElementScanIterable<T extends Element> implements CloseableIterable<T> {
  private final String elementClass;
  private final OrientBaseGraph graph;
  private final boolean polymorphic;

  public OrientElementScanIterable(
      final OrientBaseGraph graph, final String elementClass, final boolean polymorphic) {
    this.graph = graph;
    this.elementClass = elementClass;
    this.polymorphic = polymorphic;
  }

  public Iterator<T> iterator() {
    final ODatabaseDocumentInternal rawGraph = this.graph.getRawGraph();
    return new OrientElementIterator<T>(
        this.graph, new ORecordIteratorClass<ORecord>(rawGraph, elementClass, polymorphic));
  }

  public void close() {}
}
