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
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL DELETE EDGE command.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLDeleteEdge extends OCommandExecutorSQLSetAware {
  public static final String NAME = "DELETE EDGE";
  private ORecordId          rid;
  private ORecordId          from;
  private ORecordId          to;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDeleteEdge parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    init(((OCommandRequestText) iRequest).getText());

    parserRequiredKeyword("DELETE");
    parserRequiredKeyword("EDGE");

    String temp = parseOptionalWord(true);

    while (temp != null) {

      if (temp.equals("FROM")) {
        from = new ORecordId(parseRequiredWord(false));
        if (rid != null)
          throwSyntaxErrorException("FROM '" + from + "' is not allowed when specify a RID (" + rid + ")");

      } else if (temp.equals("TO")) {
        to = new ORecordId(parseRequiredWord(false));
        if (rid != null)
          throwSyntaxErrorException("TO '" + to + "' is not allowed when specify a RID (" + rid + ")");

      } else if (temp.startsWith("#")) {
        rid = new ORecordId(temp);
        if (from != null || to != null)
          throwSyntaxErrorException("Specifying the RID " + rid + " is not allowed with FROM/TO");
      }

      temp = parseOptionalWord(true);
      if (parserIsEnded())
        break;
    }

    return this;
  }

  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (from == null && to == null && rid == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    ODatabaseRecord database = getDatabase();
    if (!(database instanceof OGraphDatabase))
      database = new OGraphDatabase((ODatabaseRecordTx) database);

    final int removed;

    if (rid != null) {
      // REMOVE PUNCTUAL RID
      if (((OGraphDatabase) database).removeEdge(rid))
        removed = 1;
      else
        removed = 0;
    } else {
      // MULTIPLE EDGES
      final Set<OIdentifiable> edges;

      if (from != null && to != null)
        // REMOVE ALL THE EDGES BETWEEN VERTICES
        edges = ((OGraphDatabase) database).getEdgesBetweenVertexes(from, to);
      else if (from != null)
        // REMOVE ALL THE EDGES THAT START FROM A VERTEXES
        edges = new HashSet<OIdentifiable>(((OGraphDatabase) database).getOutEdges(from));
      else
        // REMOVE ALL THE EDGES THAT ARRIVE TO A VERTEXES
        edges = new HashSet<OIdentifiable>(((OGraphDatabase) database).getInEdges(to));

      // DELETE THE FOUND EDGES
      removed = edges.size();
      for (OIdentifiable edge : edges)
        ((OGraphDatabase) database).removeEdge(edge);
    }

    return removed;
  }

  @Override
  public String getSyntax() {
    return "DELETE EDGE <rid>|FROM <rid>|TO <rid>";
  }

}
