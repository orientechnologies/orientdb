/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;

/**
 * SQL DROP CLUSTER command: Drop a cluster from the database
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropCluster extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP    = "DROP";
  public static final String KEYWORD_CLUSTER = "CLUSTER";

  private String             clusterName;

  public OCommandExecutorSQLDropCluster parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <cluster>. Use " + getSyntax(), parserText, pos);

    clusterName = word.toString();
    if (clusterName == null)
      throw new OCommandSQLParsingException("Cluster is null. Use " + getSyntax(), parserText, pos);

    return this;
  }

  /**
   * Execute the DROP CLUSTER.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clusterName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocumentInternal database = getDatabase();

    // CHECK IF ANY CLASS IS USING IT
    final int clusterId = database.getStorage().getClusterIdByName(clusterName);
    for (OClass iClass : database.getMetadata().getSchema().getClasses()) {
      for (int i : iClass.getClusterIds()) {
        if (i == clusterId)
          // IN USE
          return false;
      }
    }

    database.dropCluster(clusterId, true);
    return true;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  protected boolean isClusterDeletable(int clusterId) {
    final ODatabaseDocument database = getDatabase();
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
    return "DROP CLUSTER <cluster>";
  }
}
