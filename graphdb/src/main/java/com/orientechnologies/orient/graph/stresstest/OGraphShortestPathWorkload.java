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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.stresstest.ODatabaseIdentifier;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import java.util.ArrayList;
import java.util.List;

/**
 * CRUD implementation of the workload.
 *
 * @author Luca Garulli
 */
public class OGraphShortestPathWorkload extends OBaseGraphWorkload {

  static final String     INVALID_FORM_MESSAGE = "SHORTESTPATH workload must be in form of L<limit>.";

  private int             limit                = -1;
  private OWorkLoadResult result               = new OWorkLoadResult();
  final List<ORID>        startingVertices     = new ArrayList<ORID>(limit > -1 ? limit : 1000);

  public OGraphShortestPathWorkload() {
    super(false);
  }

  @Override
  public String getName() {
    return "SHORTESTPATH";
  }

  @Override
  public void parseParameters(final String args) {
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
  public void execute(final int concurrencyLevel, final ODatabaseIdentifier databaseIdentifier) {
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

    executeOperation(databaseIdentifier, result, concurrencyLevel, new OCallable<Void, OBaseWorkLoadContext>() {
      @Override
      public Void call(final OBaseWorkLoadContext context) {
        final OWorkLoadContext graphContext = ((OWorkLoadContext) context);
        final OrientBaseGraph graph = graphContext.graph;

        for (int i = 0; i < startingVertices.size(); ++i) {
          final Object commandResult = graph.command(new OCommandSQL("select shortestPath(?,?, 'out')"))
              .execute(startingVertices.get(context.currentIdx), startingVertices.get(i));
        }
        result.current.incrementAndGet();

        return null;
      }
    });
  }

  @Override
  public String getPartialResult() {
    return String.format("%d%% [Shortest paths executed: %d]", ((100 * result.current.get() / result.total)), result.current.get());
  }

  @Override
  public String getFinalResult() {
    final StringBuilder buffer = new StringBuilder(getErrors());

    buffer.append(String.format("\nExecuted %d shortest paths in %.3f secs", result.current.get(), result.totalTime / 1000f));

    buffer.append(
        String.format("\n- Throughput: %.3f/sec - Avg: %.3fms/op (%dth percentile) - 99th Perc: %.3fms - 99.9th Perc: %.3fms",
            result.total * 1000 / (float) result.totalTime, result.avgNs / 1000000f, result.percentileAvg,
            result.percentile99Ns / 1000000f, result.percentile99_9Ns / 1000000f));

    return buffer.toString();
  }

  @Override
  public String getFinalResultAsJson() {
    final ODocument json = new ODocument();

    json.field("shortestPath", result.toJSON(), OType.EMBEDDED);

    return json.toString();
  }

  private char assignState(final char state, final StringBuilder number, final char c) {
    if (number.length() == 0)
      number.append("0");

    if (state == 'L')
      limit = Integer.parseInt(number.toString());

    number.setLength(0);
    return c;
  }

  @Override
  protected OBaseWorkLoadContext getContext() {
    return new OWorkLoadContext();
  }

  public int getShortestPaths() {
    return result.total;
  }

  public int getLimit() {
    return limit;
  }
}
