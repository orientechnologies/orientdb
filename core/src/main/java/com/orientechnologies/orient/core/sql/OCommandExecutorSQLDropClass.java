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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.storage.OCluster;

/**
 * SQL DROP CLASS command: Drops a class from the database. Cluster associated are removed too if are used exclusively by the
 * deleting class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP  = "DROP";
  public static final String KEYWORD_CLASS = "CLASS";

  private String             className;

  public OCommandExecutorSQLDropClass parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <class>. Use " + getSyntax(), parserText, pos);

    className = word.toString();

    return this;
  }

  /**
   * Execute the DROP CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (className == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    final OClass oClass = database.getMetadata().getSchema().getClass(className);
    if (oClass == null)
      return null;

    for (final OIndex<?> oIndex : oClass.getClassIndexes()) {
      database.getMetadata().getIndexManager().dropIndex(oIndex.getName());
    }

    final OClass superClass = oClass.getSuperClass();
    final int[] clustersToIndex = oClass.getPolymorphicClusterIds();

    final String[] clusterNames = new String[clustersToIndex.length];
    for (int i = 0; i < clustersToIndex.length; i++) {
      clusterNames[i] = database.getClusterNameById(clustersToIndex[i]);
    }

    final int clusterId = oClass.getDefaultClusterId();

    ((OSchemaProxy) database.getMetadata().getSchema()).dropClassInternal(className);
    ((OSchemaProxy) database.getMetadata().getSchema()).saveInternal();
    database.getMetadata().getSchema().reload();

    deleteDefaultCluster(clusterId);

    if (superClass == null)
      return true;

    for (final OIndex<?> oIndex : superClass.getIndexes()) {
      for (final String clusterName : clusterNames)
        oIndex.getInternal().removeCluster(clusterName);

      OLogManager.instance()
          .info(this, "Index %s is used in super class of %s and should be rebuilt.", oIndex.getName(), className);
      oIndex.rebuild();
    }

    return true;
  }

  protected void deleteDefaultCluster(int clusterId) {
    final ODatabaseRecord database = getDatabase();
    OCluster cluster = database.getStorage().getClusterById(clusterId);
    if (cluster.getName().equalsIgnoreCase(className)) {
      if (isClusterDeletable(clusterId)) {
        database.getStorage().dropCluster(clusterId, true);
      }
    }
  }

  protected boolean isClusterDeletable(int clusterId) {
    final ODatabaseRecord database = getDatabase();
    for (OClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId)
          return false;
      }
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "DROP CLASS <class>";
  }
}
