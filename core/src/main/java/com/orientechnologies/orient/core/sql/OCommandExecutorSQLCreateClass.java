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

import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;

/**
 * SQL CREATE CLASS command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE   = "CREATE";
  public static final String KEYWORD_CLASS    = "CLASS";
  public static final String KEYWORD_EXTENDS  = "EXTENDS";
  public static final String KEYWORD_ABSTRACT = "ABSTRACT";
  public static final String KEYWORD_CLUSTER  = "CLUSTER";

  private String             className;
  private OClass             superClass;
  private int[]              clusterIds;

  public OCommandExecutorSQLCreateClass parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();
    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <class>", parserText, oldPos);

    className = word.toString();
    if (className == null)
      throw new OCommandSQLParsingException("Expected <class>", parserText, oldPos);

    oldPos = pos;

    while ((pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true)) > -1) {
      final String k = word.toString();
      if (k.equals(KEYWORD_EXTENDS)) {
        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
        if (pos == -1)
          throw new OCommandSQLParsingException("Syntax error after EXTENDS for class " + className
              + ". Expected the super-class name. Use " + getSyntax(), parserText, oldPos);

        if (!database.getMetadata().getSchema().existsClass(word.toString()))
          throw new OCommandSQLParsingException("Super-class " + word + " not exists", parserText, oldPos);

        superClass = database.getMetadata().getSchema().getClass(word.toString());
      } else if (k.equals(KEYWORD_CLUSTER)) {
        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false, " =><()");
        if (pos == -1)
          throw new OCommandSQLParsingException("Syntax error after CLUSTER for class " + className
              + ". Expected the cluster id or name. Use " + getSyntax(), parserText, oldPos);

        final String[] clusterIdsAsStrings = word.toString().split(",");
        if (clusterIdsAsStrings.length > 0) {
          clusterIds = new int[clusterIdsAsStrings.length];
          for (int i = 0; i < clusterIdsAsStrings.length; ++i) {
            if (Character.isDigit(clusterIdsAsStrings[i].charAt(0)))
              // GET CLUSTER ID FROM NAME
              clusterIds[i] = Integer.parseInt(clusterIdsAsStrings[i]);
            else
              // GET CLUSTER ID
              clusterIds[i] = database.getStorage().getClusterIdByName(clusterIdsAsStrings[i]);

            if (clusterIds[i] == -1)
              throw new OCommandSQLParsingException("Cluster with id " + clusterIds[i] + " does not exists", parserText, oldPos);

            try {
              database.getStorage().getClusterById(clusterIds[i]);
            } catch (Exception e) {
              throw new OCommandSQLParsingException("Cluster with id " + clusterIds[i] + " does not exists", parserText, oldPos);
            }
          }
        }
      } else if (k.equals(KEYWORD_ABSTRACT))
        clusterIds = new int[] { -1 };
      else
        throw new OCommandSQLParsingException("Invalid keyword: " + k);

      oldPos = pos;
    }

    if (clusterIds == null) {
      final int clusterId = database.getStorage().getClusterIdByName(className);
      if (clusterId > -1) {
        clusterIds = new int[] { clusterId };
      }
    }

    return this;
  }

  /**
   * Execute the CREATE CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (className == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    if (database.getMetadata().getSchema().existsClass(className))
      throw new OCommandExecutionException("Class " + className + " already exists");

    final OClassImpl sourceClass = (OClassImpl) ((OSchemaProxy) database.getMetadata().getSchema()).createClassInternal(className,
        superClass, clusterIds);
    sourceClass.saveInternal();

    return database.getMetadata().getSchema().getClasses().size();
  }

  @Override
  public String getSyntax() {
    return "CREATE CLASS <class> [EXTENDS <super-class>] [CLUSTER <clusterId>*] [ABSTRACT]";
  }
}
