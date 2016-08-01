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
package com.orientechnologies.orient.graph.stresstest;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.OStressTesterSettings;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.List;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OGraphInsertWorkload extends OBaseGraphWorkload {

  static final String     INVALID_FORM_MESSAGE = "GRAPH INSERT workload must be in form of <vertices>F<connection-factor>.";

  private int             factor               = 80;
  private OWorkLoadResult resultVertices       = new OWorkLoadResult();
  private OWorkLoadResult resultEdges          = new OWorkLoadResult();

  public OGraphInsertWorkload() {
    connectionStrategy = OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_CONNECT;
  }

  @Override
  public String getName() {
    return "GINSERT";
  }

  @Override
  public void parseParameters(final String args) {
    final String ops = args.toUpperCase();
    char state = ' ';
    final StringBuilder number = new StringBuilder();

    for (int pos = 0; pos < ops.length(); ++pos) {
      final char c = ops.charAt(pos);

      if (c == ' ' || c == 'V' || c == 'F') {
        state = assignState(state, number, c);
      } else if (c >= '0' && c <= '9')
        number.append(c);
      else
        throw new IllegalArgumentException(
            "Character '" + c + "' is not valid on " + getName() + " workload. " + INVALID_FORM_MESSAGE);
    }
    assignState(state, number, ' ');

    if (resultVertices.total == 0)
      throw new IllegalArgumentException(INVALID_FORM_MESSAGE);
  }

  @Override
  public void execute(final OStressTesterSettings settings, final ODatabaseIdentifier databaseIdentifier) {
    final List<OBaseWorkLoadContext> contexts = executeOperation(databaseIdentifier, resultVertices, settings.concurrencyLevel,
        settings.operationsPerTransaction, new OCallable<Void, OBaseWorkLoadContext>() {
          @Override
          public Void call(final OBaseWorkLoadContext context) {
            final OWorkLoadContext graphContext = ((OWorkLoadContext) context);
            final OrientBaseGraph graph = graphContext.graph;

            final OrientVertex v = graph.addVertex(null, "_id", resultVertices.current.get());

            if (graphContext.lastVertexToConnect != null) {
              v.addEdge("E", graphContext.lastVertexToConnect);
              resultEdges.current.incrementAndGet();

              graphContext.lastVertexEdges++;

              if (graphContext.lastVertexEdges > factor) {
                graphContext.lastVertexEdges = 0;
                graphContext.lastVertexToConnect = v;
              }
            } else
              graphContext.lastVertexToConnect = v;

            resultVertices.current.incrementAndGet();

            if( settings.operationsPerTransaction > 0 && context.currentIdx % settings.operationsPerTransaction == 0 ){
              graph.commit();
              graph.begin();
            }

            return null;
          }
        });

    final OrientBaseGraph graph = settings.operationsPerTransaction > 0 ? getGraph(databaseIdentifier)
        : getGraphNoTx(databaseIdentifier);
    try {
      // CONNECTED ALL THE SUB GRAPHS
      OrientVertex lastVertex = null;
      for (OBaseWorkLoadContext context : contexts) {
        if (lastVertex != null)
          lastVertex.addEdge("E", ((OWorkLoadContext) context).lastVertexToConnect);

        lastVertex = ((OWorkLoadContext) context).lastVertexToConnect;
      }
    } finally {
      graph.shutdown();
    }

  }

  @Override
  public String getPartialResult() {
    return String.format("%d%% [Vertices: %d - Edges: %d]", ((100 * resultVertices.current.get() / resultVertices.total)),
        resultVertices.current.get(), resultEdges.current.get());
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format("- Created %d vertices and %d edges in %.3f secs", resultVertices.current.get(),
        resultEdges.current.get(), resultVertices.totalTime / 1000f));

    buffer.append(resultVertices.toOutput(1));

    return buffer.toString();
  }

  @Override
  public String getFinalResultAsJson() {
    final ODocument json = new ODocument();

    json.field("type", getName());

    json.field("vertices", resultVertices.toJSON(), OType.EMBEDDED);
    json.field("edges", resultEdges.toJSON(), OType.EMBEDDED);

    return json.toJSON("");
  }

  private char assignState(final char state, final StringBuilder number, final char c) {
    if (number.length() == 0)
      number.append("0");

    if (state == 'V')
      resultVertices.total = Integer.parseInt(number.toString());
    else if (state == 'F')
      factor = Integer.parseInt(number.toString());

    number.setLength(0);
    return c;
  }

  @Override
  protected OBaseWorkLoadContext getContext() {
    return new OBaseGraphWorkload.OWorkLoadContext();
  }

  public int getVertices() {
    return resultVertices.total;
  }

  public int getFactor() {
    return factor;
  }
}
