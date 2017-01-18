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

import java.util.Map;

/**
 * SQL CREATE CLUSTER command: Creates a new cluster.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateCluster extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE  = "CREATE";
  public static final String KEYWORD_BLOB    = "BLOB";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  public static final String KEYWORD_ID      = "ID";

  private String clusterName;
  private int     requestedId = -1;
  private boolean blob        = false;

  public OCommandExecutorSQLCreateCluster parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      final ODatabaseDocumentInternal database = getDatabase();

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword(KEYWORD_CREATE);
      String nextWord = parserRequiredWord(true);
      if (nextWord.equals("BLOB")) {
        parserRequiredKeyword(KEYWORD_CLUSTER);
        blob = true;
      } else if (!nextWord.equals(KEYWORD_CLUSTER)) {
        throw new OCommandSQLParsingException("Invalid Syntax: " + queryText);
      }

      clusterName = parserRequiredWord(false);
      clusterName = decodeClassName(clusterName);
      if (!clusterName.isEmpty() && Character.isDigit(clusterName.charAt(0)))
        throw new IllegalArgumentException("Cluster name cannot begin with a digit");

      String temp = parseOptionalWord(true);

      while (temp != null) {
        if (temp.equals(KEYWORD_ID)) {
          requestedId = Integer.parseInt(parserRequiredWord(false));
        }

        temp = parseOptionalWord(true);
        if (parserIsEnded())
          break;
      }

    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase().getConfiguration().getValueAsLong(OGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  /**
   * Execute the CREATE CLUSTER.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clusterName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocument database = getDatabase();

    final int clusterId = database.getClusterIdByName(clusterName);
    if (clusterId > -1)
      throw new OCommandSQLParsingException("Cluster '" + clusterName + "' already exists");

    if (blob) {
      if (requestedId == -1) {
        return database.addBlobCluster(clusterName);
      } else {
        throw new OCommandExecutionException("Request id not supported by blob cluster creation.");
      }
    } else {
      if (requestedId == -1) {
        return database.addCluster(clusterName);
      } else {
        return database.addCluster(clusterName, requestedId, null);
      }
    }
  }

  @Override
  public String getUndoCommand() {
    return "drop cluster " + clusterName;
  }

  @Override
  public String getSyntax() {
    return "CREATE CLUSTER <name> [ID <requested cluster id>]";
  }
}
