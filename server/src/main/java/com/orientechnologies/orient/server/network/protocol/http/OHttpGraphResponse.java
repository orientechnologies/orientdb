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
package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.executor.OResult;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Graph wrapper to format the response as graph.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OHttpGraphResponse extends OHttpResponse {
  public OHttpGraphResponse(final OHttpResponse iWrapped) {
    super(iWrapped.getOutputStream(), iWrapped.httpVersion, iWrapped.additionalHeaders, iWrapped.characterSet, iWrapped.serverInfo,
        iWrapped.sessionId, iWrapped.callbackFunction, iWrapped.keepAlive, iWrapped.connection);
  }

  public void writeRecords(final Object iRecords, final String iFetchPlan, String iFormat, final String accept,
      final Map<String, Object> iAdditionalProperties, final String mode) throws IOException {
    if (iRecords == null)
      return;

    if (!mode.equalsIgnoreCase("graph")) {
      super.writeRecords(iRecords, iFetchPlan, iFormat, accept, iAdditionalProperties, mode);
      return;
    }

    if (accept != null && accept.contains("text/csv"))
      throw new IllegalArgumentException("Graph mode cannot accept '" + accept + "'");

    ODatabaseDocument graph = ODatabaseRecordThreadLocal.instance().get();

    try {
      // DIVIDE VERTICES FROM EDGES
      final Set<OVertex> vertices = new HashSet<>();

      final Iterator<Object> iIterator = OMultiValue.getMultiValueIterator(iRecords);
      while (iIterator.hasNext()) {
        Object entry = iIterator.next();

        if (entry != null && entry instanceof OResult && ((OResult) entry).isElement()) {

          entry = ((OResult) entry).getElement().get();

        } else if (entry == null || !(entry instanceof OIdentifiable)) {
          // IGNORE IT
          continue;
        }

        entry = ((OIdentifiable) entry).getRecord();

        if (entry == null || !(entry instanceof OIdentifiable))
          // IGNORE IT
          continue;

        if (entry instanceof OElement) {
          OElement element = (OElement) entry;
          if (element.isVertex()) {
            vertices.add(element.asVertex().get());
          } else if (element.isEdge()) {
            OEdge edge = element.asEdge().get();
            vertices.add(edge.getTo());
            vertices.add(edge.getFrom());
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
      for (OVertex vertex : vertices) {
        json.beginObject();

        json.writeAttribute("@rid", vertex.getIdentity());
        json.writeAttribute("@class", vertex.getSchemaType().get().getName());

        // ADD ALL THE PROPERTIES
        for (String field : vertex.getPropertyNames()) {
          final Object v = vertex.getProperty(field);
          if (v != null)
            json.writeAttribute(field, v);
        }
        json.endObject();
      }
      json.endCollection();

      // WRITE EDGES
      json.beginCollection("edges");
      Set<ORID> edgeRids = new HashSet<ORID>();
      for (OVertex vertex : vertices) {
        for (OEdge e : vertex.getEdges(ODirection.BOTH)) {
          OEdge edge = (OEdge) e;
          if (edgeRids.contains(e.getIdentity())) {
            continue;
          }
          if (!vertices.contains(edge.getVertex(ODirection.OUT)) || !vertices.contains(edge.getVertex(ODirection.IN)))
            // ONE OF THE 2 VERTICES ARE NOT PART OF THE RESULT SET: DISCARD IT
            continue;

          edgeRids.add(edge.getIdentity());

          json.beginObject();
          json.writeAttribute("@rid", edge.getIdentity());
          json.writeAttribute("@class", edge.getSchemaType().map(x -> x.getName()).orElse(null));

          json.writeAttribute("out", edge.getVertex(ODirection.OUT).getIdentity());
          json.writeAttribute("in", edge.getVertex(ODirection.IN).getIdentity());

          for (String field : edge.getPropertyNames()) {
            final Object v = edge.getProperty(field);
            if (v != null)
              json.writeAttribute(field, v);
          }

          json.endObject();
        }
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

      send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, buffer.toString(), null);
    } finally {
      graph.close();
    }
  }
}
