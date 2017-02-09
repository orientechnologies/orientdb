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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.OStressTesterSettings;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import java.util.List;
import java.util.Random;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OGraphInsertWorkload extends OBaseGraphWorkload {
  private enum STRATEGIES {
    LAST, RANDOM, SUPERNODE
  }

  static final String     INVALID_FORM_MESSAGE = "GRAPH INSERT workload must be in form of <vertices>F<connection-factor>.";

  private int             factor               = 80;
  private OWorkLoadResult resultVertices       = new OWorkLoadResult();
  private OWorkLoadResult resultEdges          = new OWorkLoadResult();
  private STRATEGIES      strategy             = STRATEGIES.LAST;

  public OGraphInsertWorkload() {
    connectionStrategy = OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST;
  }

  @Override
  public String getName() {
    return "GINSERT";
  }

  @Override
  protected void init(OBaseWorkLoadContext context) {
    synchronized (getClass()) {
      final OrientBaseGraph g = ((OWorkLoadContext) context).graph;
      if (g.getVertexType(className) == null) {
        final OrientVertexType c = g.createVertexType(className);
        c.createProperty("_id", OType.LONG);
        c.createProperty("ts", OType.DATETIME);
      }
    }
  }

  @Override
  public void parseParameters(final String args) {
    final String ops = args.toUpperCase();
    char state = ' ';
    final StringBuilder value = new StringBuilder();

    boolean strategy = false;
    for (int pos = 0; pos < ops.length(); ++pos) {
      final char c = ops.charAt(pos);

      if (c == ' ' || c == 'V' || c == 'F' || (c == 'S' && !strategy)) {
        if (c == 'S')
          strategy = true;
        state = assignState(state, value, c);
      } else
        value.append(c);
    }
    assignState(state, value, ' ');

    if (resultVertices.total == 0)
      throw new IllegalArgumentException(INVALID_FORM_MESSAGE);
  }

  @Override
  public void execute(final OStressTesterSettings settings, final ODatabaseIdentifier databaseIdentifier) {
    connectionStrategy = settings.loadBalancing;

    final List<OBaseWorkLoadContext> contexts = executeOperation(databaseIdentifier, resultVertices, settings, new OCallable<Void, OBaseWorkLoadContext>() {
          @Override
          public Void call(final OBaseWorkLoadContext context) {
            final OWorkLoadContext graphContext = ((OWorkLoadContext) context);
            final OrientBaseGraph graph = graphContext.graph;

            final OrientVertex v = graph.addVertex("class:" + className, "_id", resultVertices.current.get(), "ts",
                System.currentTimeMillis());

            if (graphContext.lastVertexToConnect != null) {
              v.addEdge("E", graphContext.lastVertexToConnect);
              resultEdges.current.incrementAndGet();

              graphContext.lastVertexEdges++;

              if (graphContext.lastVertexEdges > factor) {
                graphContext.lastVertexEdges = 0;
                if (strategy == STRATEGIES.LAST)
                  graphContext.lastVertexToConnect = v;
                else if (strategy == STRATEGIES.RANDOM) {
                  do {
                    final int[] totalClusters = graph.getVertexBaseType().getClusterIds();
                    final int randomCluster = totalClusters[new Random().nextInt(totalClusters.length)];
                    long totClusterRecords = graph.getRawGraph().countClusterElements(randomCluster);
                    if (totClusterRecords > 0) {
                      final ORecordId randomRid = new ORecordId(randomCluster, new Random().nextInt((int) totClusterRecords));
                      graphContext.lastVertexToConnect = graph.getVertex(randomRid);
                      break;
                    }

                  } while (true);
                } else if (strategy == STRATEGIES.SUPERNODE) {
                  final int[] totalClusters = graph.getVertexBaseType().getClusterIds();
                  final int firstCluster = totalClusters[0];
                  long totClusterRecords = graph.getRawGraph().countClusterElements(firstCluster);
                  if (totClusterRecords > 0) {
                    final ORecordId randomRid = new ORecordId(firstCluster, 0);
                    graphContext.lastVertexToConnect = graph.getVertex(randomRid);
                  }
                }
              }
            } else
              graphContext.lastVertexToConnect = v;

            resultVertices.current.incrementAndGet();
            return null;
          }
        });

    final OrientBaseGraph graph = settings.operationsPerTransaction > 0 ? getGraph(databaseIdentifier)
        : getGraphNoTx(databaseIdentifier);
    try {
      // CONNECTED ALL THE SUB GRAPHS
      OrientVertex lastVertex = null;
      for (OBaseWorkLoadContext context : contexts) {
        for (int retry = 0; retry < 100; ++retry)
          try {
            if (lastVertex != null)
              lastVertex.addEdge("E", ((OWorkLoadContext) context).lastVertexToConnect);

            lastVertex = ((OWorkLoadContext) context).lastVertexToConnect;
          } catch (ONeedRetryException e) {
            if (lastVertex.getIdentity().isPersistent())
              lastVertex.reload();

            if (((OWorkLoadContext) context).lastVertexToConnect.getIdentity().isPersistent())
              ((OWorkLoadContext) context).lastVertexToConnect.reload();
          }
      }
    } finally {
      graph.shutdown();
    }
  }

  protected void manageNeedRetryException(OBaseWorkLoadContext context, ONeedRetryException e) {
    if (((OWorkLoadContext) context).lastVertexToConnect.getIdentity().isPersistent())
      ((OWorkLoadContext) context).lastVertexToConnect.reload();
  }

  @Override
  public String getPartialResult() {
    return String.format("%d%% [Vertices: %d - Edges: %d (conflicts=%d)]",
        ((100 * resultVertices.current.get() / resultVertices.total)), resultVertices.current.get(), resultEdges.current.get(),
        resultVertices.conflicts.get());
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
    else if (state == 'S')
      strategy = STRATEGIES.valueOf(number.toString().toUpperCase());

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
