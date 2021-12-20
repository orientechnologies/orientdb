/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import java.util.Map;

/**
 * SQL DROP CLUSTER command: Drop a cluster from the database
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropCluster extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_CLUSTER = "CLUSTER";

  private String clusterName;

  public OCommandExecutorSQLDropCluster parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      final StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
        throw new OCommandSQLParsingException(
            "Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);

      pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER))
        throw new OCommandSQLParsingException(
            "Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
      if (pos == -1)
        throw new OCommandSQLParsingException(
            "Expected <cluster>. Use " + getSyntax(), parserText, pos);

      clusterName = word.toString();
      if (clusterName == null)
        throw new OCommandSQLParsingException(
            "Cluster is null. Use " + getSyntax(), parserText, pos);

      clusterName = decodeClassName(clusterName);
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /** Execute the DROP CLUSTER. */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clusterName == null)
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocumentInternal database = getDatabase();

    // CHECK IF ANY CLASS IS USING IT
    final int clusterId = database.getClusterIdByName(clusterName);
    for (OClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId)
          // IN USE
          return false;
      }
    }

    database.dropCluster(clusterId);
    return true;
  }

  @Override
  public long getDistributedTimeout() {
    if (clusterName != null && getDatabase().existsCluster(clusterName))
      return 10 * getDatabase().countClusterElements(clusterName);

    return getDatabase()
        .getConfiguration()
        .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_LONG_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  protected boolean isClusterDeletable(int clusterId) {
    final ODatabaseDocument database = getDatabase();
    for (OClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId) return false;
      }
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "DROP CLUSTER <cluster>";
  }
}
