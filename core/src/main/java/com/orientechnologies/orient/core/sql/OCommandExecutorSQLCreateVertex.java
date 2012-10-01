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

import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;

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
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    init(((OCommandRequestText) iRequest).getText());

    String className = null;

    parserRequiredKeyword("CREATE");
    parserRequiredKeyword("VERTEX");

    String temp = parseOptionalWord(true);

    while (temp != null) {
      if (temp.equals("CLUSTER")) {
        clusterName = parserRequiredWord(false);

      } else if (temp.equals("SET")) {
        fields = new LinkedHashMap<String, Object>();
        parseSetFields(fields);

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

    ODatabaseRecord database = getDatabase();
    if (!(database instanceof OGraphDatabase))
      database = new OGraphDatabase((ODatabaseRecordTx) database);

    final ODocument vertex = ((OGraphDatabase) database).createVertex(clazz.getName());

    OSQLHelper.bindParameters(vertex, fields, new OCommandParameters(iArgs));

    if (clusterName != null)
      vertex.save(clusterName);
    else
      vertex.save();

    return vertex;
  }

  @Override
  public String getSyntax() {
    return "CREATE VERTEX [<class>] [CLUSTER <cluster>] [SET <field> = <expression>[,]*]";
  }

}
