/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.graph;

import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase.DIRECTION;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;

/**
 * Dijkstra's algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDijkstra extends OSQLFunctionPathFinder<Float> {
  public static final String NAME = "dijkstra";
  private static final Float MIN  = new Float(0f);

  private String             paramWeightFieldName;

  public OSQLFunctionDijkstra() {
    super(NAME, 3, 4);
  }

  public Object execute(OIdentifiable iCurrentRecord, final Object[] iParameters, OCommandContext iContext) {
    final ODatabaseRecord currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();
    db = (OGraphDatabase) (currentDatabase instanceof OGraphDatabase ? currentDatabase : new OGraphDatabase(
        (ODatabaseRecordTx) currentDatabase));

    final ORecordInternal<?> record = (ORecordInternal<?>) (iCurrentRecord != null ? iCurrentRecord.getRecord() : null);

    paramSourceVertex = (OIdentifiable) OSQLHelper.getValue(iParameters[0], record, iContext);
    paramDestinationVertex = (OIdentifiable) OSQLHelper.getValue(iParameters[1], record, iContext);
    paramWeightFieldName = (String) OSQLHelper.getValue(iParameters[2], record, iContext);
    if (iParameters.length > 3)
      paramDirection = DIRECTION.valueOf(iParameters[3].toString().toUpperCase());

    return super.execute(iParameters, iContext);
  }

  public String getSyntax() {
    return "Syntax error: dijkstra(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<direction>])";
  }

  @Override
  protected Float getShortestDistance(final OIdentifiable destination) {
    if (destination == null)
      return Float.MAX_VALUE;

    final Float d = distance.get(destination);
    return d == null ? Float.MAX_VALUE : d;
  }

  @Override
  protected Float getMinimumDistance() {
    return MIN;
  }

  protected Float getDistance(final OIdentifiable node, final OIdentifiable target) {
    final Set<OIdentifiable> edges = db.getEdgesBetweenVertexes(node, target);
    if (!edges.isEmpty()) {
      final ODocument e = edges.iterator().next().getRecord();
      if (e != null) {
        final Object fieldValue = e.field(paramWeightFieldName);
        if (fieldValue != null)
          if (fieldValue instanceof Float)
            return (Float) fieldValue;
          else if (fieldValue instanceof Number)
            return ((Number) fieldValue).floatValue();
      }
    }
    return MIN;
  }

  @Override
  protected Float sumDistances(final Float iDistance1, final Float iDistance2) {
    return iDistance1.floatValue() + iDistance2.floatValue();
  }
}
