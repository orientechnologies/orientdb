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
package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Graph wrapper to format the response as graph.
 *
 * @author Luca Garulli
 */
public class OHttpGraphResponse extends OHttpResponse {
  public OHttpGraphResponse(final OHttpResponse iWrapped) {
    super(iWrapped.getOutputStream(), iWrapped.httpVersion, iWrapped.additionalHeaders, iWrapped.characterSet, iWrapped.serverInfo,
        iWrapped.sessionId, iWrapped.callbackFunction, iWrapped.keepAlive, iWrapped.connection);
  }

  public void writeRecords(final Object iRecords, final String iFetchPlan, String iFormat, final String accept,
      final Map<String, Object> iAdditionalProperties, final String mode) throws IOException {
    if (iRecords == null){
      send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, "", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
      return;
    }


    if (!mode.equalsIgnoreCase("graph")) {
      super.writeRecords(iRecords, iFetchPlan, iFormat, accept, iAdditionalProperties, mode);
      return;
    }

    if (accept != null && accept.contains("text/csv"))
      throw new IllegalArgumentException("Graph mode cannot accept '" + accept + "'");

    final OrientGraphNoTx graph = (OrientGraphNoTx) OrientGraphFactory.getNoTxGraphImplFactory()
        .getGraph((ODatabaseDocumentTx) ODatabaseRecordThreadLocal.INSTANCE.get());

    try {
      // DIVIDE VERTICES FROM EDGES
      final Set<OrientVertex> vertices = new HashSet<OrientVertex>();
      final Set<OrientEdge> edges = new HashSet<OrientEdge>();

      final Iterator<Object> iIterator = OMultiValue.getMultiValueIterator(iRecords);
      while (iIterator.hasNext()) {
        Object entry = iIterator.next();
        if (entry == null || !(entry instanceof OIdentifiable))
          // IGNORE IT
          continue;

        entry = ((OIdentifiable) entry).getRecord();

        if (entry == null || !(entry instanceof OIdentifiable))
          // IGNORE IT
          continue;

        if (entry instanceof ODocument) {
          OClass schemaClass = ((ODocument) entry).getSchemaClass();
          if (schemaClass != null && schemaClass.isVertexType())
            vertices.add(graph.getVertex(entry));
          else if (schemaClass != null && schemaClass.isEdgeType()) {
            OrientEdge edge = graph.getEdge(entry);
            vertices.add(graph.getVertex(edge.getVertex(Direction.IN)));
            vertices.add(graph.getVertex(edge.getVertex(Direction.OUT)));
            edges.add(edge);
          } else
            // IGNORE IT
            continue;
        }
      }

      final StringWriter buffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(buffer, "");
      json.beginObject();
      json.beginObject("graph");

      // WRITE VERTICES
      json.beginCollection("vertices");
      for (OrientVertex vertex : vertices) {
        json.beginObject();

        json.writeAttribute("@rid", vertex.getIdentity());
        json.writeAttribute("@class", vertex.getRecord().getClassName());

        // ADD ALL THE VERTEX'S EDGES
        for (Edge e : vertex.getEdges(Direction.BOTH))
          edges.add((OrientEdge) e);

        // ADD ALL THE PROPERTIES
        for (String field : vertex.getPropertyKeys()) {
          final Object v = vertex.getProperty(field);
          if (v != null)
            json.writeAttribute(field, v);
        }
        json.endObject();
      }
      json.endCollection();

      // WRITE EDGES
      json.beginCollection("edges");
      for (OrientEdge edge : edges) {
        if (!vertices.contains(edge.getVertex(Direction.OUT)) || !vertices.contains(edge.getVertex(Direction.IN)))
          // ONE OF THE 2 VERTICES ARE NOT PART OF THE RESULT SET: DISCARD IT
          continue;

        json.beginObject();
        json.writeAttribute("@rid", edge.getIdentity());
        json.writeAttribute("@class", edge.getRecord().getClassName());

        json.writeAttribute("out", edge.getVertex(Direction.OUT).getId());
        json.writeAttribute("in", edge.getVertex(Direction.IN).getId());

        for (String field : edge.getPropertyKeys()) {
          final Object v = edge.getProperty(field);
          if (v != null)
            json.writeAttribute(field, v);
        }

        json.endObject();
      }
      json.endCollection();

      if (iAdditionalProperties != null) {
        for (Map.Entry<String, Object> entry : iAdditionalProperties.entrySet()) {

          final Object v = entry.getValue();
          if (OMultiValue.isMultiValue(v)) {
            json.beginCollection(-1, true, entry.getKey());
            formatMultiValue(OMultiValue.getMultiValueIterator(v), buffer, null);
            json.endCollection(-1, true);
          } else
            json.writeAttribute(entry.getKey(), v);

          if (Thread.currentThread().isInterrupted())
            break;

        }
      }

      json.endObject();
      json.endObject();

      send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
    } finally {
      graph.shutdown();
    }
  }
}
