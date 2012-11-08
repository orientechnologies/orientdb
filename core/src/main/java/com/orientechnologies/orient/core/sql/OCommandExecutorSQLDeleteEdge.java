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
package com.orientechnologies.orient.core.sql;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;

/**
 * SQL DELETE EDGE command.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLDeleteEdge extends OCommandExecutorSQLSetAware implements OCommandResultListener {
  public static final String NAME    = "DELETE EDGE";
  private ORecordId          rid;
  private ORecordId          from;
  private ORecordId          to;
  private int                removed = 0;
  private ODatabaseRecord    database;
  private OCommandRequest    query;
  private OSQLFilter         compiledFilter;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDeleteEdge parse(final OCommandRequest iRequest) {
    database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    init(((OCommandRequestText) iRequest).getText());

    parserRequiredKeyword("DELETE");
    parserRequiredKeyword("EDGE");

    OClass clazz = null;

    String temp = parseOptionalWord(true);
    while (temp != null) {

      if (temp.equals("FROM")) {
        from = new ORecordId(parserRequiredWord(false));
        if (rid != null)
          throwSyntaxErrorException("FROM '" + from + "' is not allowed when specify a RID (" + rid + ")");

      } else if (temp.equals("TO")) {
        to = new ORecordId(parserRequiredWord(false));
        if (rid != null)
          throwSyntaxErrorException("TO '" + to + "' is not allowed when specify a RID (" + rid + ")");

      } else if (temp.startsWith("#")) {
        rid = new ORecordId(temp);
        if (from != null || to != null)
          throwSyntaxErrorException("Specifying the RID " + rid + " is not allowed with FROM/TO");

      } else if (temp.equals(KEYWORD_WHERE)) {
        if (clazz == null)
          // ASSIGN DEFAULT CLASS
          clazz = database.getMetadata().getSchema().getClass(OGraphDatabase.EDGE_CLASS_NAME);

        final String condition = parserGetCurrentPosition() > -1 ? " " + parserText.substring(parserGetCurrentPosition()) : "";

        compiledFilter = OSQLEngine.getInstance().parseCondition(condition, getContext(), KEYWORD_WHERE);
        break;

      } else if (temp.length() > 0) {
        // GET/CHECK CLASS NAME
        clazz = database.getMetadata().getSchema().getClass(temp);
        if (clazz == null)
          throw new OCommandSQLParsingException("Class '" + temp + " was not found");
      }

      temp = parseOptionalWord(true);
      if (parserIsEnded())
        break;
    }

    if (from == null && to == null && rid == null && clazz == null && compiledFilter == null)
      // DELETE ALL THE EDGES
      query = database.command(new OSQLAsynchQuery<ODocument>("select from E", this));

    return this;
  }

  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (from == null && to == null && rid == null && query == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    database = getDatabase();
    if (!(database instanceof OGraphDatabase))
      database = new OGraphDatabase((ODatabaseRecordTx) database);

    if (rid != null) {
      // REMOVE PUNCTUAL RID
      if (((OGraphDatabase) database).removeEdge(rid))
        removed = 1;
    } else {
      // MULTIPLE EDGES
      final Set<OIdentifiable> edges;

      if (query == null) {
        // SELECTIVE TARGET
        if (from != null && to != null)
          // REMOVE ALL THE EDGES BETWEEN VERTICES
          edges = ((OGraphDatabase) database).getEdgesBetweenVertexes(from, to);
        else if (from != null)
          // REMOVE ALL THE EDGES THAT START FROM A VERTEXES
          edges = new HashSet<OIdentifiable>(((OGraphDatabase) database).getOutEdges(from));
        else if (to != null)
          // REMOVE ALL THE EDGES THAT ARRIVE TO A VERTEXES
          edges = new HashSet<OIdentifiable>(((OGraphDatabase) database).getInEdges(to));
        else
          throw new OCommandExecutionException("Invalid target");

        if (compiledFilter != null) {
          // ADDITIONAL FILTERING
          for (Iterator<OIdentifiable> it = edges.iterator(); it.hasNext();) {
            final OIdentifiable edge = it.next();
            if (!(Boolean) compiledFilter.evaluate((ODocument) edge.getRecord(), null, context))
              it.remove();
          }
        }

        // DELETE THE FOUND EDGES
        removed = edges.size();
        for (OIdentifiable edge : edges)
          ((OGraphDatabase) database).removeEdge(edge);
      } else if (query != null)
        // TARGET IS A CLASS + OPTIONAL CONDITION
        query.execute(iArgs);
      else
        throw new OCommandExecutionException("Invalid target");
    }

    return removed;
  }

  /**
   * Delete the current edge.
   */
  public boolean result(final Object iRecord) {
    final OIdentifiable id = (OIdentifiable) iRecord;

    if (compiledFilter != null) {
      // ADDITIONAL FILTERING
      if (!(Boolean) compiledFilter.evaluate((ODocument) id.getRecord(), null, context))
        return false;
    }

    if (id.getIdentity().isValid()) {

      if (((OGraphDatabase) database).removeEdge(id)) {
        removed++;
        return true;
      }
    }

    return false;
  }

  @Override
  public String getSyntax() {
    return "DELETE EDGE <rid>|FROM <rid>|TO <rid>|<[<class>] [WHERE <conditions>]>";
  }
}
