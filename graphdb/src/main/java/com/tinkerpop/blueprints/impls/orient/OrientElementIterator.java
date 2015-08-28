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

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.tinkerpop.blueprints.Element;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class OrientElementIterator<T extends Element> implements Iterator<T> {

  private final Iterator<?>     itty;
  private final OrientBaseGraph graph;

  public OrientElementIterator(final OrientBaseGraph graph, final Iterator<?> itty) {
    this.itty = itty;
    this.graph = graph;
  }

  public boolean hasNext() {
    return this.itty.hasNext();
  }

  @SuppressWarnings("unchecked")
  public T next() {
    OrientElement currentElement = null;

    if (!hasNext())
      throw new NoSuchElementException();

    Object current = itty.next();

    if (null == current)
      throw new NoSuchElementException();

    if (current instanceof OIdentifiable)
      current = ((OIdentifiable) current).getRecord();

    if (current instanceof ODocument) {
      final ODocument currentDocument = (ODocument) current;

      if (currentDocument.getInternalStatus() == ODocument.STATUS.NOT_LOADED)
        currentDocument.load();

      OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(currentDocument);
      if (immutableClass == null)
        throw new IllegalArgumentException(
            "Cannot determine the graph element type because the document class is null. Probably this is a projection, use the EXPAND() function");

      if (immutableClass.isEdgeType())
        currentElement = new OrientEdge(graph, currentDocument);
      else
        currentElement = new OrientVertex(graph, currentDocument);
    }

    return (T) currentElement;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}
