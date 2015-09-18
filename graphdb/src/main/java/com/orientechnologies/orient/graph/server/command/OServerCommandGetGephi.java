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
package com.orientechnologies.orient.graph.server.command;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OServerCommandGetGephi extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|gephi/*" };

  public OServerCommandGetGephi() {
  }

  public OServerCommandGetGephi(final OServerCommandConfiguration iConfig) {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(
        iRequest.url,
        4,
        "Syntax error: gephi/<database>/<language>/<query-text>[/<limit>][/<fetchPlan>].<br>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

    final String language = urlParts[2];
    final String text = urlParts[3];
    final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4]) : 20;
    final String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;

    iRequest.data.commandInfo = "Gephi";
    iRequest.data.commandDetail = text;

    final ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);

    final OModifiableBoolean shutdownFlag = new OModifiableBoolean();
    final OrientGraph graph = OGraphCommandExecutorSQLFactory.getGraph(false, shutdownFlag);
    try {

      final Iterable<OrientElement> vertices;
      if (language.equals("sql"))
        vertices = graph.command(new OSQLSynchQuery<OrientVertex>(text, limit).setFetchPlan(fetchPlan)).execute();
      else if (language.equals("gremlin")) {
        List<Object> result = new ArrayList<Object>();
        OGremlinHelper.execute(graph, text, null, null, result, null, null);

        vertices = new ArrayList<OrientElement>(result.size());

        for (Object o : result) {
          ((ArrayList<OrientElement>) vertices).add(graph.getVertex(o));
        }
      } else
        throw new IllegalArgumentException("Language '" + language + "' is not supported. Use 'sql' or 'gremlin'");

      sendRecordsContent(iRequest, iResponse, vertices, fetchPlan);

    } finally {
      if (graph != null && shutdownFlag.getValue())
        graph.shutdown();

      if (db != null)
        db.close();
    }

    return false;
  }

  protected void sendRecordsContent(final OHttpRequest iRequest, final OHttpResponse iResponse, Iterable<OrientElement> iRecords,
      String iFetchPlan) throws IOException {
    final StringWriter buffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(buffer, OHttpResponse.JSON_FORMAT);
    json.setPrettyPrint(true);

    generateGraphDbOutput(iRecords, json);

    iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_JSON, buffer.toString(), null);
  }

  protected void generateGraphDbOutput(final Iterable<OrientElement> iVertices, final OJSONWriter json) throws IOException {
    if (iVertices == null)
      return;

    // CREATE A SET TO SPEED UP SEARCHES ON VERTICES
    final Set<OrientVertex> vertexes = new HashSet<OrientVertex>();
    final Set<OrientEdge> edges = new HashSet<OrientEdge>();

    for (OrientElement id : iVertices)
      if (id instanceof OrientVertex)
        vertexes.add((OrientVertex) id);
      else
        edges.add((OrientEdge) id);

    for (OrientVertex vertex : vertexes) {
      json.resetAttributes();
      json.beginObject(0, false, null);
      json.beginObject(1, false, "an");
      json.beginObject(2, false, vertex.getIdentity());

      // ADD ALL THE EDGES
      for (Edge e : vertex.getEdges(Direction.BOTH))
        edges.add((OrientEdge) e);

      // ADD ALL THE PROPERTIES
      for (String field : vertex.getPropertyKeys()) {
        final Object v = vertex.getProperty(field);
        if (v != null)
          json.writeAttribute(3, false, field, v);
      }
      json.endObject(2, false);
      json.endObject(1, false);
      json.endObject(0, false);
      json.newline();
    }

    for (OrientEdge edge : edges) {

      json.resetAttributes();
      json.beginObject();
      json.beginObject(1, false, "ae");
      json.beginObject(2, false, edge.getId());
      json.writeAttribute(3, false, "directed", false);

      json.writeAttribute(3, false, "source", edge.getVertex(Direction.IN).getId());
      json.writeAttribute(3, false, "target", edge.getVertex(Direction.OUT).getId());

      for (String field : edge.getPropertyKeys()) {
        final Object v = edge.getProperty(field);
        if (v != null)
          json.writeAttribute(3, false, field, v);
      }

      json.endObject(2, false);
      json.endObject(1, false);
      json.endObject(0, false);
      json.newline();
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
