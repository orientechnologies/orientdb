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
package com.orientechnologies.orient.graph.gremlin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Executes a GREMLIN expression as function of SQL engine.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionGremlin extends OSQLFunctionAbstract {
  public static final String  NAME = "gremlin";
  private List<Object>        result;
  private Map<Object, Object> currentParameters;

  public OSQLFunctionGremlin() {
    super(NAME, 1, 1);
  }

  public Object execute(final OIdentifiable iCurrentRecord, final Object[] iParameters, final OCommandContext iContext) {
    if (!(iCurrentRecord instanceof ODocument))
      // NOT DOCUMENT OR GRAPHDB? IGNORE IT
      return null;

    final OGraphDatabase db = OGremlinHelper.getGraphDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());

    if (result == null)
      result = new ArrayList<Object>();

    final Object scriptResult = OGremlinHelper.execute(db, (String) iParameters[0], null, currentParameters, result,
        new OGremlinHelper.OGremlinCallback() {

          @Override
          public boolean call(ScriptEngine iEngine, OrientGraph iGraph) {
            final ODocument document = (ODocument) iCurrentRecord;
            if (db.isVertex(document)) {
              // VERTEX TYPE, CREATE THE BLUEPRINTS'S WRAPPER
              OrientVertex graphElement = (OrientVertex) new OrientElementIterable<OrientVertex>(iGraph, Arrays
                  .asList(new ODocument[] { document })).iterator().next();
              iEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("current", graphElement);

            } else if (db.isEdge(document)) {
              // EDGE TYPE, CREATE THE BLUEPRINTS'S WRAPPER
              OrientEdge graphElement = (OrientEdge) new OrientElementIterable<OrientEdge>(iGraph, Arrays
                  .asList(new ODocument[] { document })).iterator().next();
              iEngine.getBindings(ScriptContext.ENGINE_SCOPE).put("current", graphElement);
            } else

              // UNKNOWN CLASS: IGNORE IT
              return false;

            return true;
          }
        }, null);

    return scriptResult;
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "Syntax error: gremlin(<gremlin-expression>)";
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  @Override
  public Object getResult() {
    return result;
  }
}
