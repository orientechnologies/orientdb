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
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionConfigurableAbstract;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Hi-level function that return the vertices starting from the current record.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionVertices extends OSQLFunctionConfigurableAbstract {
  public static final String NAME = "vertices";

  public OSQLFunctionVertices() {
    super(NAME, 1, 2);
  }

  public Object execute(final OIdentifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParameters,
      final OCommandContext iContext) {
    final OrientBaseGraph graph = OGraphCommandExecutorSQLFactory.getGraph();

    final Direction direction = Direction
        .valueOf(OStringSerializerHelper.getStringContent(iParameters[0].toString().toUpperCase()));
    final String[] labels;
    if (iParameters.length > 1 && iParameters[1] != null)
      labels = OMultiValue.array(iParameters[1], String.class);
    else
      labels = null;

    if (iCurrentRecord == null) {
      return OSQLEngine.foreachRecord(new OCallable<Object, OIdentifiable>() {
        @Override
        public Object call(final OIdentifiable iArgument) {
          return getVertices(graph, iArgument, direction, labels);
        }
      }, iCurrentResult, iContext);
    } else
      return getVertices(graph, iCurrentRecord.getRecord(), direction, labels);

  }

  private Object getVertices(final OrientBaseGraph graph, final OIdentifiable iRecord, final Direction direction,
      final String[] labels) {
    final ODocument rec = iRecord.getRecord();

    if (rec.getSchemaClass() != null)
      if (rec.getSchemaClass().isSubClassOf(OrientVertex.CLASS_NAME)) {
        // VERTEX
        final OrientVertex vertex = graph.getVertex(rec);
        return vertex.getVertices(direction, labels);

      } else if (rec.getSchemaClass().isSubClassOf(OrientEdge.CLASS_NAME)) {
        // EDGE
        final OrientEdge edge = graph.getEdge(rec);
        return edge.getVertex(direction);

      }
    return null;
  }

  public String getSyntax() {
    return "Syntax error: vertices(<direction> [,<labels>])";
  }
}
