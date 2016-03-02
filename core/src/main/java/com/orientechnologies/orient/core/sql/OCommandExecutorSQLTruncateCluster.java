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
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.util.Map;

/**
 * SQL TRUNCATE CLUSTER command: Truncates an entire record cluster.
 *
 * @author Luca Garulli
 */
public class OCommandExecutorSQLTruncateCluster extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_TRUNCATE = "TRUNCATE";
  public static final String KEYWORD_CLUSTER  = "CLUSTER";
  private String clusterName;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLTruncateCluster parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_TRUNCATE + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserText, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected cluster name. Use " + getSyntax(), parserText, oldPos);

    clusterName = word.toString();

    final ODatabaseDocument database = getDatabase();
    if (database.getClusterIdByName(clusterName) == -1)
      throw new OCommandSQLParsingException("Cluster '" + clusterName + "' not found", parserText, oldPos);
    return this;
  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clusterName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocumentInternal database = getDatabase();

    final int clusterId = database.getClusterIdByName(clusterName);
    if (clusterId < 0) {
      throw new ODatabaseException("Cluster with name " + clusterName + " does not exist");
    }

    final OSchema schema = database.getMetadata().getSchema();
    final OClass clazz = schema.getClassByClusterId(clusterId);
    if (clazz == null) {
      final OStorage storage = database.getStorage();
      final OCluster cluster = storage.getClusterById(clusterId);

      if (cluster == null) {
        throw new ODatabaseException("Cluster with name " + clusterName + " does not exist");
      }

      try {
        cluster.truncate();
      } catch (IOException ioe) {
        throw new ODatabaseException("Error during truncation of cluster with name " + clusterName, ioe);
      }
    } else {
      clazz.truncateCluster(clusterName);
    }

    return true;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getSyntax() {
    return "TRUNCATE CLUSTER <cluster-name>";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }
}
