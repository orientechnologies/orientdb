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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSetAware;
import com.orientechnologies.orient.core.sql.OCommandParameters;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * SQL CREATE VERTEX command.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLCreateVertex extends OCommandExecutorSQLSetAware {
  public static final String            NAME = "CREATE VERTEX";
  private OClass                        clazz;
  private String                        clusterName;
  private LinkedHashMap<String, Object> fields;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLCreateVertex parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();

    init((OCommandRequestText) iRequest);

    String className = null;

    parserRequiredKeyword("CREATE");
    parserRequiredKeyword("VERTEX");

    String temp = parseOptionalWord(true);

    while (temp != null) {
      if (temp.equals("CLUSTER")) {
        clusterName = parserRequiredWord(false);

      } else if (temp.equals(KEYWORD_SET)) {
        fields = new LinkedHashMap<String, Object>();
        parseSetFields(fields);

      } else if (temp.equals(KEYWORD_CONTENT)) {
        parseContent();

      } else if (className == null && temp.length() > 0)
        className = temp;

      temp = parserOptionalWord(true);
      if (parserIsEnded())
        break;
    }

    if (className == null)
      // ASSIGN DEFAULT CLASS
      className = "V";

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

    final OrientVertex vertex = graph.addTemporaryVertex(clazz.getName());

    if (fields != null)
      // EVALUATE FIELDS
      for (Entry<String, Object> f : fields.entrySet()) {
        if (f.getValue() instanceof OSQLFunctionRuntime)
          fields.put(f.getKey(), ((OSQLFunctionRuntime) f.getValue()).getValue(vertex.getRecord(), context));
      }

    OSQLHelper.bindParameters(vertex.getRecord(), fields, new OCommandParameters(iArgs), context);

    if (content != null)
      vertex.getRecord().merge(content, true, false);

    if (clusterName != null)
      vertex.save(clusterName);
    else
      vertex.save();

    return vertex.getRecord();
  }

  @Override
  public String getSyntax() {
    return "CREATE VERTEX [<class>] [CLUSTER <cluster>] [SET <field> = <expression>[,]*]";
  }

}
