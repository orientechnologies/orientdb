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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OGraphInsertWorkload extends OBaseGraphWorkload {

  static final String     INVALID_FORM_MESSAGE = "GRAPH INSERT workload must be in form of <vertices>F<connection-factor>.";

  private int             vertices             = 0;
  private int             factor               = 80;
  private OWorkLoadResult resultVertices       = new OWorkLoadResult();
  private OWorkLoadResult resultEdges          = new OWorkLoadResult();

  @Override
  public String getName() {
    return "GRAPHINSERT";
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

    if (vertices == 0)
      throw new IllegalArgumentException(INVALID_FORM_MESSAGE);
  }

  @Override
  public void execute(final int concurrencyLevel, final ODatabaseIdentifier databaseIdentifier) {
    // TODO: aggregation, shortest path, hard path, neighbors, neighbors2, look also at XDBench
    executeOperation(databaseIdentifier, vertices, concurrencyLevel, new OCallable<Void, OBaseWorkLoadContext>() {
      OrientVertex lastVertexToConnect;
      int          lastVertexEdges;

      @Override
      public Void call(final OBaseWorkLoadContext context) {
        final OrientBaseGraph graph = ((OBaseGraphWorkload.OWorkLoadContext) context).graph;

        final OrientVertex v = graph.addVertex(null, "_id", resultVertices.current.get());

        if (lastVertexToConnect != null) {
          v.addEdge("E", lastVertexToConnect);
          resultEdges.current.incrementAndGet();

          lastVertexEdges++;

          if (lastVertexEdges > factor) {
            lastVertexEdges = 0;
            lastVertexToConnect = v;
          }
        } else
          lastVertexToConnect = v;

        resultVertices.current.incrementAndGet();

        return null;
      }
    });
  }

  @Override
  public String getPartialResult() {
    return String.format("%d%% [Vertices: %d - Edges: %d]", ((100 * resultVertices.current.get() / vertices)),
        resultVertices.current.get(), resultEdges.current.get());
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format("\nCreated %d vertices and %d edges in %.3f secs", resultVertices.total, resultEdges.total,
        resultVertices.totalTime / 1000f));

    buffer.append(String.format(
        "\n- Vertices Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th Perc: %.3fms - 99.9th Perc: %.3fms",
        resultVertices.total * 1000 / (float) resultVertices.totalTime, resultVertices.avgNs / 1000000f,
        resultVertices.percentileAvg, resultVertices.percentile99Ns / 1000000f, resultVertices.percentile99_9Ns / 1000000f));

    buffer.append(String.format(
        "\n- Edges    Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th Perc: %.3fms - 99.9th Perc: %.3fms",
        resultEdges.total * 1000 / (float) resultEdges.totalTime, resultEdges.avgNs / 1000000f, resultEdges.percentileAvg,
        resultEdges.percentile99Ns / 1000000f, resultEdges.percentile99_9Ns / 1000000f));

    return buffer.toString();
  }

  @Override
  public String getFinalResultAsJson() {
    final ODocument json = new ODocument();

    json.field("vertices", resultVertices.toJSON(), OType.EMBEDDED);
    json.field("edges", resultEdges.toJSON(), OType.EMBEDDED);

    return json.toString();
  }

  private char assignState(final char state, final StringBuilder number, final char c) {
    if (number.length() == 0)
      number.append("0");

    if (state == 'V')
      vertices = Integer.parseInt(number.toString());
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
    return vertices;
  }

  public int getFactor() {
    return factor;
  }
}
