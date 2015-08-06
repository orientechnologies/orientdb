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
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateCluster extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE  = "CREATE";
  public static final String KEYWORD_CLUSTER = "CLUSTER";
  public static final String KEYWORD_ID      = "ID";

  private String             clusterName;
  private int                requestedId     = -1;

  public OCommandExecutorSQLCreateCluster parse(final OCommandRequest iRequest) {
    final ODatabaseDocumentInternal database = getDatabase();

    init((OCommandRequestText) iRequest);

    parserRequiredKeyword(KEYWORD_CREATE);
    parserRequiredKeyword(KEYWORD_CLUSTER);

    clusterName = parserRequiredWord(false);
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

    return this;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
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

    if (requestedId == -1) {
      return database.addCluster(clusterName);
    } else {
      return database.addCluster(clusterName, requestedId, null);
    }
  }

  @Override
  public String getSyntax() {
    return "CREATE CLUSTER <name> [ID <requested cluster id>]";
  }
}
