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
package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLRetryAbstract;
import com.orientechnologies.orient.core.sql.OCommandParameters;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * SQL CREATE EDGE command.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLCreateEdge extends OCommandExecutorSQLRetryAbstract implements OCommandDistributedReplicateRequest {
  public static final String            NAME = "CREATE EDGE";

  private String                        from;
  private String                        to;
  private OClass                        clazz;
  private String                        clusterName;
  private LinkedHashMap<String, Object> fields;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLCreateEdge parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();

    init((OCommandRequestText) iRequest);

    parserRequiredKeyword("CREATE");
    parserRequiredKeyword("EDGE");

    String className = null;

    String temp = parseOptionalWord(true);

    while (temp != null) {
      if (temp.equals("CLUSTER")) {
        clusterName = parserRequiredWord(false);

      } else if (temp.equals(KEYWORD_FROM)) {
        from = parserRequiredWord(false, "Syntax error", " =><,\r\n");

      } else if (temp.equals("TO")) {
        to = parserRequiredWord(false, "Syntax error", " =><,\r\n");

      } else if (temp.equals(KEYWORD_SET)) {
        fields = new LinkedHashMap<String, Object>();
        parseSetFields(fields);

      } else if (temp.equals(KEYWORD_CONTENT)) {
        parseContent();

      } else if (temp.equals(KEYWORD_RETRY)) {
        parseRetry();

      } else if (className == null && temp.length() > 0)
        className = temp;

      temp = parseOptionalWord(true);
      if (parserIsEnded())
        break;
    }

    if (className == null)
      // ASSIGN DEFAULT CLASS
      className = "E";

    // GET/CHECK CLASS NAME
    clazz = database.getMetadata().getSchema().getClass(className);
    if (clazz == null)
      throw new OCommandSQLParsingException("Class " + className + " was not found");

    return this;
  }

  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clazz == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    return OGraphCommandExecutorSQLFactory.runInTx(new OGraphCommandExecutorSQLFactory.GraphCallBack<List<Object>>() {
      @Override
      public List<Object> call(OrientBaseGraph graph) {
        final Set<OIdentifiable> fromIds = OSQLEngine.getInstance().parseRIDTarget(graph.getRawGraph(), from, context);
        final Set<OIdentifiable> toIds = OSQLEngine.getInstance().parseRIDTarget(graph.getRawGraph(), to, context);

        // CREATE EDGES
        final List<Object> edges = new ArrayList<Object>();
        for (OIdentifiable from : fromIds) {
          final OrientVertex fromVertex = graph.getVertex(from);
          if (fromVertex == null)
            throw new OCommandExecutionException("Source vertex '" + from + "' not exists");

          for (OIdentifiable to : toIds) {
            final OrientVertex toVertex;
            if (from.equals(to)) {
              toVertex = fromVertex;
            } else {
              toVertex = graph.getVertex(to);
            }

            final String clsName = clazz.getName();

            if (fields != null)
              // EVALUATE FIELDS
              for (Entry<String, Object> f : fields.entrySet()) {
                if (f.getValue() instanceof OSQLFunctionRuntime)
                  fields.put(f.getKey(), ((OSQLFunctionRuntime) f.getValue()).getValue(to, null, context));
              }

            OrientEdge edge = null;
            for (int r = 0; r < retry; ++r) {
              try {
                edge = fromVertex.addEdge(null, toVertex, clsName, clusterName, fields);

                if (fields != null && !fields.isEmpty()) {
                  if (!edge.getRecord().getIdentity().isValid())
                    edge.convertToDocument();

                  OSQLHelper.bindParameters(edge.getRecord(), fields, new OCommandParameters(iArgs), context);
                }

                if (content != null) {
                  if (!edge.getRecord().getIdentity().isValid())
                    // LIGHTWEIGHT EDGE, TRANSFORM IT BEFORE
                    edge.convertToDocument();
                  edge.getRecord().merge(content, true, false);
                }

                edge.save(clusterName);

                // OK
                break;

              } catch (OConcurrentModificationException e) {
                if (r + 1 >= retry)
                  // NO RETRY; PROPAGATE THE EXCEPTION
                  throw e;

                // RETRY?
                if (wait > 0)
                  try {
                    Thread.sleep(wait);
                  } catch (InterruptedException e1) {
                  }

                // RELOAD LAST VERSION
                fromVertex.getRecord().reload(null, true);
                toVertex.getRecord().reload(null, true);
              }
            }

            edges.add(edge);
          }
        }

        return edges;
      }
    });
  }

  @Override
  public String getSyntax() {
    return "CREATE EDGE [<class>] [CLUSTER <cluster>] FROM <rid>|(<query>|[<rid>]*) TO <rid>|(<query>|[<rid>]*) [SET <field> = <expression>[,]*]|CONTENT {<JSON>} [RETRY <retry> [WAIT <pauseBetweenRetriesInMs]]";
  }
}
