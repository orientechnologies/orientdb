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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase.DIRECTION;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.OSQLHelper;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionShortestPath extends OSQLFunctionPathFinder<Integer> {
  public static final String   NAME     = "shortestPath";
  private static final Integer MIN      = new Integer(0);
  private static final Integer DISTANCE = new Integer(1);

  public OSQLFunctionShortestPath() {
    super(NAME, 2, 3);
  }

  public Object execute(final OIdentifiable iCurrentRecord, final Object[] iParameters, final OCommandContext iContext) {
    final ODatabaseRecord currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();
    db = (OGraphDatabase) (currentDatabase instanceof OGraphDatabase ? currentDatabase : new OGraphDatabase(
        (ODatabaseRecordTx) currentDatabase));

    final ORecordInternal<?> record = (ORecordInternal<?>) (iCurrentRecord != null ? iCurrentRecord.getRecord() : null);

    paramSourceVertex = (OIdentifiable) OSQLHelper.getValue(iParameters[0], record, iContext);
    paramDestinationVertex = (OIdentifiable) OSQLHelper.getValue(iParameters[1], record, iContext);
    if (iParameters.length > 2)
      paramDirection = DIRECTION.valueOf(iParameters[2].toString().toUpperCase());

    return super.execute(iParameters, iContext);
  }

  public String getSyntax() {
    return "Syntax error: shortestPath(<sourceVertex>, <destinationVertex>, [<direction>])";
  }

  @Override
  protected Integer getShortestDistance(final OIdentifiable destination) {
    if (destination == null)
      return Integer.MAX_VALUE;

    final Integer d = distance.get(destination);
    return d == null ? Integer.MAX_VALUE : d;
  }

  @Override
  protected Integer getMinimumDistance() {
    return MIN;
  }

  protected Integer getDistance(final OIdentifiable node, final OIdentifiable target) {
    return DISTANCE;
  }

  @Override
  protected Integer sumDistances(final Integer iDistance1, final Integer iDistance2) {
    return iDistance1.intValue() + iDistance2.intValue();
  }
}
