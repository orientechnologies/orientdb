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
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Dijkstra's algorithm describes how to find the cheapest path from one node to another node in a
 * directed weighted graph.
 *
 * <p>The first parameter is source record. The second parameter is destination record. The third
 * parameter is a name of property that represents 'weight'.
 *
 * <p>If property is not defined in edge or is null, distance between vertexes are 0.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @deprecated see {@link
 *     com.orientechnologies.orient.core.sql.functions.graph.OSQLFunctionDijkstra} instead
 */
@Deprecated
public class OSQLFunctionDijkstra extends OSQLFunctionPathFinder {
  public static final String NAME = "dijkstra";

  private String paramWeightFieldName;

  public OSQLFunctionDijkstra() {
    super(NAME, 3, 4);
  }

  public LinkedList<OrientVertex> execute(
      final Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      final OCommandContext iContext) {
    return new OSQLFunctionAstar()
        .execute(this, iCurrentRecord, iCurrentResult, toAStarParams(iParams), iContext);
  }

  private Object[] toAStarParams(Object[] iParams) {
    Object[] result = new Object[4];
    result[0] = iParams[0];
    result[1] = iParams[1];
    result[2] = iParams[2];
    Map<String, Object> options = new HashMap<String, Object>();
    options.put("emptyIfMaxDepth", true);
    if (iParams.length > 3) {
      options.put("direction", iParams[3]);
    }
    result[3] = options;
    return result;
  }

  private LinkedList<OrientVertex> internalExecute(final OCommandContext iContext) {
    return super.execute(iContext);
  }

  public String getSyntax() {
    return "dijkstra(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<direction>])";
  }

  protected float getDistance(final OrientVertex node, final OrientVertex target) {
    return -1; // not used anymore
  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }
}
