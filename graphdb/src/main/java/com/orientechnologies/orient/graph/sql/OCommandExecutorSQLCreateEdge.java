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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSetAware;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * SQL CREATE EDGE command.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLCreateEdge extends OCommandExecutorSQLSetAware {
  public static final String            NAME = "CREATE EDGE";
  private String                        from;
  private String                        to;
  private OClass                        clazz;
  private String                        clusterName;
  private LinkedHashMap<String, Object> fields;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLCreateEdge parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

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

    final OrientBaseGraph graph = OGraphCommandExecutorSQLFactory.getGraph();

    final ORecordId[] fromIds = parseTarget(graph.getRawGraph(), from);
    final ORecordId[] toIds = parseTarget(graph.getRawGraph(), to);

    // CREATE EDGES
    final List<Object> edges = new ArrayList<Object>();
    for (ORecordId from : fromIds) {
      final OrientVertex fromVertex = (OrientVertex) graph.getVertex(from);
      if (fromVertex == null)
        throw new OCommandExecutionException("Source vertex '" + from + "' not exists");

      for (ORecordId to : toIds) {
        final OrientVertex toVertex = (OrientVertex) graph.getVertex(to);

        final String clsName = clazz.getName();

        if (fields != null)
          // EVALUATE FIELDS
          for (Entry<String, Object> f : fields.entrySet()) {
            if (f.getValue() instanceof OSQLFunctionRuntime)
              fields.put(f.getKey(), ((OSQLFunctionRuntime) f.getValue()).getValue(to, context));
          }

        final OrientEdge edge = fromVertex.addEdge(null, toVertex, clsName, clusterName, fields);

        if (content != null) {
          if (!edge.getRecord().getIdentity().isValid())
            // LIGHTWEIGHT EDGE, TRANSFORM IT BEFORE
            edge.convertToDocument();
          edge.getRecord().merge(content, true, false);
        }

        edge.save(clusterName);

        edges.add(edge);
      }
    }

    return edges;
  }

  @Override
  public String getSyntax() {
    return "CREATE EDGE [<class>] [CLUSTER <cluster>] FROM <rid>|(<query>|[<rid>]*) TO <rid>|(<query>|[<rid>]*) [SET <field> = <expression>[,]*]|CONTENT {<JSON>}";
  }

  protected ORecordId[] parseTarget(final ODatabaseRecord database, final String iTarget) {
    final ORecordId[] ids;
    if (iTarget.startsWith("(")) {
      // SUB-QUERY
      final List<OIdentifiable> result = database.query(new OSQLSynchQuery<Object>(iTarget.substring(1, iTarget.length() - 1)));
      if (result == null || result.isEmpty())
        ids = new ORecordId[0];
      else {
        ids = new ORecordId[result.size()];
        for (int i = 0; i < result.size(); ++i)
          ids[i] = new ORecordId(result.get(i).getIdentity());
      }
    } else if (iTarget.startsWith("[")) {
      // COLLECTION OF RIDS
      final String[] idsAsStrings = iTarget.substring(1, iTarget.length() - 1).split(",");
      ids = new ORecordId[idsAsStrings.length];
      for (int i = 0; i < idsAsStrings.length; ++i)
        ids[i] = new ORecordId(idsAsStrings[i]);
    } else
      // SINGLE RID
      ids = new ORecordId[] { new ORecordId(iTarget) };
    return ids;
  }

}
