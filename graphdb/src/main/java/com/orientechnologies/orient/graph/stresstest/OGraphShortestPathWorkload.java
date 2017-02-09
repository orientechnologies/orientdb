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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.orientechnologies.orient.stresstest.OStressTesterSettings;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OGraphShortestPathWorkload extends OBaseGraphWorkload {

  static final String      INVALID_FORM_MESSAGE = "SHORTESTPATH workload must be in form of L<limit>.";

  private int              limit                = -1;
  private OWorkLoadResult  result               = new OWorkLoadResult();
  private final AtomicLong totalDepth           = new AtomicLong();
  private final AtomicLong maxDepth             = new AtomicLong();
  private final AtomicLong notConnected         = new AtomicLong();
  private final List<ORID> startingVertices     = new ArrayList<ORID>(limit > -1 ? limit : 1000);

  public OGraphShortestPathWorkload() {
    connectionStrategy = OStorageRemote.CONNECTION_STRATEGY.ROUND_ROBIN_REQUEST;
  }

  @Override
  public String getName() {
    return "GSP";
  }

  @Override
  public void parseParameters(final String args) {
    if (args == null)
      return;

    final String ops = args.toUpperCase();
    char state = 'L';
    final StringBuilder number = new StringBuilder();

    for (int pos = 0; pos < ops.length(); ++pos) {
      final char c = ops.charAt(pos);

      if (c == ' ' || c == 'L') {
        state = assignState(state, number, c);
      } else if (c >= '0' && c <= '9')
        number.append(c);
      else
        throw new IllegalArgumentException(
            "Character '" + c + "' is not valid on " + getName() + " workload. " + INVALID_FORM_MESSAGE);
    }
    assignState(state, number, ' ');

    result.total = 1;
  }

  @Override
  public void execute(final OStressTesterSettings settings, final ODatabaseIdentifier databaseIdentifier) {
    connectionStrategy = settings.loadBalancing;

    // RETRIEVE THE STARTING VERTICES
    final OrientGraphNoTx g = getGraphNoTx(databaseIdentifier);
    try {
      for (OIdentifiable id : g.getRawGraph().browseClass("V")) {
        startingVertices.add(id.getIdentity());
        if (limit > -1 && startingVertices.size() >= limit)
          break;
      }
    } finally {
      g.shutdown();
    }
    result.total = startingVertices.size();

    executeOperation(databaseIdentifier, result, settings, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        final OWorkLoadContext graphContext = ((OWorkLoadContext) context);
        final OrientBaseGraph graph = graphContext.graph;

        for (int i = 0; i < startingVertices.size(); ++i) {
          final Iterable<OrientVertex> commandResult = graph.command(new OCommandSQL("select shortestPath(?,?, 'both')"))
              .execute(startingVertices.get(context.currentIdx), startingVertices.get(i));

          for (OrientVertex v : commandResult) {
            Collection depth = v.getRecord().field("shortestPath");
            if (depth != null && !depth.isEmpty()) {
              totalDepth.addAndGet(depth.size());

              long max = maxDepth.get();
              while (depth.size() > max) {
                if (maxDepth.compareAndSet(max, depth.size()))
                  break;
                max = maxDepth.get();
              }
            } else
              notConnected.incrementAndGet();
          }
        }
        result.current.incrementAndGet();

        return null;
      }
    });
  }

  @Override
  public String getPartialResult() {
    return String.format("%d%% [Shortest paths blocks (block size=%d) executed: %d/%d]",
        ((100 * result.current.get() / result.total)), startingVertices.size(), result.current.get(), startingVertices.size());
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format("- Executed %d shortest paths in %.3f secs", result.current.get(), result.totalTime / 1000f));
    buffer.append(String.format("\n- Path depth: maximum %d, average %.3f, not connected %d", maxDepth.get(),
        totalDepth.get() / (float) startingVertices.size() / (float) startingVertices.size(), notConnected.get()));
    buffer.append(result.toOutput(1));

    return buffer.toString();
  }

  @Override
  public String getFinalResultAsJson() {
    final ODocument json = new ODocument();

    json.field("type", getName());

    json.field("shortestPath", result.toJSON(), OType.EMBEDDED);

    return json.toJSON("");
  }

  private char assignState(final char state, final StringBuilder number, final char c) {
    if (number.length() == 0)
      number.append("0");

    if (state == 'L')
      limit = Integer.parseInt(number.toString());

    number.setLength(0);
    return c;
  }

  public int getShortestPaths() {
    return result.total;
  }

  public int getLimit() {
    return limit;
  }
}
