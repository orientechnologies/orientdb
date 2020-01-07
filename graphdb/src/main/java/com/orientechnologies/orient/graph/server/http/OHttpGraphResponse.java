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
package com.orientechnologies.orient.graph.server.http;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.network.protocol.http.OChunkedResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.io.*;
import java.net.Socket;
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
    super(iWrapped.getOutputStream(), iWrapped.getHttpVersion(), iWrapped.getAdditionalHeaders(), iWrapped.getCharacterSet(),
        iWrapped.getServerInfo(), iWrapped.getSessionId(), iWrapped.getCallbackFunction(), iWrapped.isKeepAlive(),
        iWrapped.getConnection());
  }

  public void writeRecords(final Object iRecords, final String iFetchPlan, String iFormat, final String accept,
      final Map<String, Object> iAdditionalProperties, final String mode) throws IOException {
    if (iRecords == null) {
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
        .getGraph(ODatabaseRecordThreadLocal.instance().get());

    try {
      //all the edges in the result-set
      final Set<OrientEdge> rsEdges = new HashSet<OrientEdge>();

      //all the vertices sent
      final Set<OrientVertex> vertices = new HashSet<OrientVertex>();

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
            rsEdges.add(edge);
            vertices.add(graph.getVertex(edge.getVertex(Direction.IN)));
            vertices.add(graph.getVertex(edge.getVertex(Direction.OUT)));
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
      if (rsEdges.isEmpty()) {
        //no explicit edges in the result-set, let's calculate them from vertices
        Set<ORID> edgeRids = new HashSet<ORID>();
        for (OrientVertex vertex : vertices) {
          for (Edge e : vertex.getEdges(Direction.BOTH)) {
            OrientEdge edge = (OrientEdge) e;
            if (edgeRids.contains(((OrientEdge) e).getIdentity())) {
              continue;
            }
            if (!vertices.contains(edge.getVertex(Direction.OUT)) || !vertices.contains(edge.getVertex(Direction.IN)))
              // ONE OF THE 2 VERTICES ARE NOT PART OF THE RESULT SET: DISCARD IT
              continue;

            edgeRids.add(edge.getIdentity());

            writeEdge(edge, json);
          }
        }
      } else {
        //edges are explicitly in the RS, only send them
        for (OrientEdge edge : rsEdges) {
          if (!vertices.contains(edge.getVertex(Direction.OUT)) || !vertices.contains(edge.getVertex(Direction.IN)))
            // ONE OF THE 2 VERTICES ARE NOT PART OF THE RESULT SET: DISCARD IT
            continue;

          writeEdge(edge, json);
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
      graph.shutdown();
    }
  }

  private void writeEdge(OrientEdge edge, OJSONWriter json) throws IOException {
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

  @Override
  public void send(final int iCode, final String iReason, final String iContentType, final Object iContent, final String iHeaders)
      throws IOException {
    if (isSendStarted()) {
      // AVOID TO SEND RESPONSE TWICE
      return;
    }
    setSendStarted(true);

    if (getCallbackFunction() != null) {
      setContent(getCallbackFunction() + "(" + iContent + ")");
      setContentType("text/javascript");
    } else {
      if (getContent() == null || getContent().length() == 0) {
        setContent(iContent != null ? iContent.toString() : null);
      }
      if (getContentType() == null || getContentType().length() == 0) {
        setContentType(iContentType);
      }
    }

    final boolean empty = getContent() == null || getContent().length() == 0;

    if (this.getCode() > 0) {
      writeStatus(this.getCode(), iReason);
    } else {
      writeStatus(empty && iCode == 200 ? 204 : iCode, iReason);
    }
    writeHeaders(getContentType(), isKeepAlive());

    if (iHeaders != null) {
      writeLine(iHeaders);
    }

    if (getSessionId() != null)
      writeLine("Set-Cookie: " + OHttpUtils.OSESSIONID + "=" + getSessionId() + "; Path=/; HttpOnly");

    byte[] binaryContent = null;
    if (!empty) {
      if (getContentEncoding() != null && getContentEncoding().equals(OHttpUtils.CONTENT_ACCEPT_GZIP_ENCODED)) {
        binaryContent = compress(getContent());
      } else {
        binaryContent = getContent().getBytes(utf8);
      }
    }

    writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (empty ? 0 : binaryContent.length));

    writeLine(null);

    if (binaryContent != null) {
      getOut().write(binaryContent);
    }

    flush();
  }

  @Override
  public void writeStatus(final int iStatus, final String iReason) throws IOException {
    writeLine(getHttpVersion() + " " + iStatus + " " + iReason);
  }

  @Override
  public void sendStream(final int iCode, final String iReason, final String iContentType, InputStream iContent, long iSize)
      throws IOException {
    sendStream(iCode, iReason, iContentType, iContent, iSize, null, null);
  }

  @Override
  public void sendStream(final int iCode, final String iReason, final String iContentType, InputStream iContent, long iSize,
      final String iFileName) throws IOException {
    sendStream(iCode, iReason, iContentType, iContent, iSize, iFileName, null);
  }

  @Override
  public void sendStream(final int iCode, final String iReason, final String iContentType, InputStream iContent, long iSize,
      final String iFileName, Map<String, String> additionalHeaders) throws IOException {
    writeStatus(iCode, iReason);
    writeHeaders(iContentType);
    writeLine("Content-Transfer-Encoding: binary");

    if (iFileName != null) {
      writeLine("Content-Disposition: attachment; filename=\"" + iFileName + "\"");
    }

    if (additionalHeaders != null) {
      for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
        writeLine(String.format("%s: %s", entry.getKey(), entry.getValue()));
      }
    }
    if (iSize < 0) {
      // SIZE UNKNOWN: USE A MEMORY BUFFER
      final ByteArrayOutputStream o = new ByteArrayOutputStream();
      if (iContent != null) {
        int b;
        while ((b = iContent.read()) > -1) {
          o.write(b);
        }
      }

      byte[] content = o.toByteArray();

      iContent = new ByteArrayInputStream(content);
      iSize = content.length;
    }

    writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (iSize));
    writeLine(null);

    if (iContent != null) {
      int b;
      while ((b = iContent.read()) > -1) {
        getOut().write(b);
      }
    }

    flush();
  }

  @Override
  public void sendStream(final int iCode, final String iReason, final String iContentType, final String iFileName,
      final OCallable<Void, OChunkedResponse> iWriter) throws IOException {
    writeStatus(iCode, iReason);
    writeHeaders(iContentType);
    writeLine("Content-Transfer-Encoding: binary");
    writeLine("Transfer-Encoding: chunked");

    if (iFileName != null) {
      writeLine("Content-Disposition: attachment; filename=\"" + iFileName + "\"");
    }

    writeLine(null);

    final OChunkedResponse chunkedOutput = new OChunkedResponse(this);
    iWriter.call(chunkedOutput);
    chunkedOutput.close();

    flush();
  }

  @Override
  protected void checkConnection() throws IOException {
    final Socket socket;
    if (getConnection().getProtocol() == null || getConnection().getProtocol().getChannel() == null)
      socket = null;
    else
      socket = getConnection().getProtocol().getChannel().socket;
    if (socket == null || socket.isClosed() || socket.isInputShutdown()) {
      OLogManager.instance()
          .debug(this, "[OHttpResponse] found and removed pending closed channel %d (%s)", getConnection(), socket);
      throw new IOException("Connection is closed");
    }
  }
}
