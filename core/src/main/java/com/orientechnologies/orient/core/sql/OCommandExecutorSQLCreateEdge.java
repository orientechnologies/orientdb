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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
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
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

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

    init(((OCommandRequestText) iRequest).getText());

    String className = null;

    parserRequiredKeyword("CREATE");
    parserRequiredKeyword("EDGE");

    String temp = parseOptionalWord(true);

    while (temp != null) {
      if (temp.equals("CLUSTER")) {
        clusterName = parserRequiredWord(false);

      } else if (temp.equals("FROM")) {
        from = parserRequiredWord(false, "Syntax error", " =><,\r\n");

      } else if (temp.equals("TO")) {
        to = parserRequiredWord(false, "Syntax error", " =><,\r\n");

      } else if (temp.equals("SET")) {
        fields = new LinkedHashMap<String, Object>();
        parseSetFields(fields);

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

    ODatabaseRecord database = getDatabase();
    if (!(database instanceof OGraphDatabase))
      database = new OGraphDatabase((ODatabaseRecordTx) database);

    final ORecordId[] fromIds = parseTarget(database, from);
    final ORecordId[] toIds = parseTarget(database, to);

    // CREATE EDGES
    List<ODocument> edges = new ArrayList<ODocument>();
    for (ORecordId from : fromIds) {
      for (ORecordId to : toIds) {
        final ODocument edge = ((OGraphDatabase) database).createEdge(from, to, clazz.getName());
        OSQLHelper.bindParameters(edge, fields, new OCommandParameters(iArgs));
        edge.save(clusterName);

        edges.add(edge);
      }
    }

    return edges;
  }

  @Override
  public String getSyntax() {
    return "CREATE EDGE [<class>] [CLUSTER <cluster>] FROM <rid>|(<query>|[<rid>]*) TO <rid>|(<query>|[<rid>]*) [SET <field> = <expression>[,]*]";
  }

  protected ORecordId[] parseTarget(ODatabaseRecord database, final String iTarget) {
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
