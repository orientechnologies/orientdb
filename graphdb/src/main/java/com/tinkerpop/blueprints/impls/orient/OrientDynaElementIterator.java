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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
class OrientDynaElementIterator implements Iterator<Object> {

  private final Iterator<?>     itty;
  private final OrientBaseGraph graph;

  public OrientDynaElementIterator(final OrientBaseGraph graph, final Iterator<?> itty) {
    this.itty = itty;
    this.graph = graph;
  }

  public boolean hasNext() {
    return this.itty.hasNext();
  }

  public Object next() {
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

      final OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(currentDocument);
      if (schemaClass != null && schemaClass.isSubClassOf(graph.getEdgeBaseType()))
        currentElement = graph.getEdge(currentDocument);
      else
        // RETURN VERTEX IN ALL THE CASES, EVEN FOR PROJECTED DOCUMENTS
        currentElement = graph.getVertex(currentDocument);
    }

    if(currentElement==null){
      return current;
    }

    return currentElement;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}
