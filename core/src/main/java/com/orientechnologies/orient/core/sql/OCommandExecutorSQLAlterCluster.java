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
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterCluster extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_ALTER   = "ALTER";
  public static final String KEYWORD_CLUSTER = "CLUSTER";

  protected String           clusterName;
  protected int              clusterId       = -1;
  protected ATTRIBUTES       attribute;
  protected String           value;

  public OCommandExecutorSQLAlterCluster parse(final OCommandRequest iRequest) {
    final ODatabaseDocument database = getDatabase();

    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLUSTER + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <cluster-name>. Use " + getSyntax(), parserText, oldPos);

    clusterName = word.toString();

    final Pattern p = Pattern.compile("([0-9]*)");
    final Matcher m = p.matcher(clusterName);
    if (m.matches())
      clusterId = Integer.parseInt(clusterName);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Missing cluster attribute to change. Use " + getSyntax(), parserText, oldPos);

    final String attributeAsString = word.toString();

    try {
      attribute = OCluster.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new OCommandSQLParsingException("Unknown class attribute '" + attributeAsString + "'. Supported attributes are: "
          + Arrays.toString(OCluster.ATTRIBUTES.values()), parserText, oldPos, e);
    }

    value = parserText.substring(pos + 1).trim();

    if (value.length() == 0)
      throw new OCommandSQLParsingException("Missing property value to change for attribute '" + attribute + "'. Use "
          + getSyntax(), parserText, oldPos);

    if (value.equalsIgnoreCase("null"))
      value = null;

    return this;
  }

  /**
   * Execute the ALTER CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (attribute == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final OCluster cluster = getCluster();

    if (cluster == null)
      throw new OCommandExecutionException("Cluster '" + clusterName + "' not found");

    if (clusterId > -1 && clusterName.equals(String.valueOf(clusterId))) {
      clusterName = cluster.getName();
    } else {
      clusterId = cluster.getId();
    }

    Object result;
    try {
      result = cluster.set(attribute, value);
      final OStorage storage = getDatabase().getStorage();
      if (storage instanceof OLocalPaginatedStorage)
        ((OLocalPaginatedStorage) storage).synch();
    } catch (IOException ioe) {
      throw new OCommandExecutionException("Error altering cluster '" + clusterName + "'", ioe);
    }

    return result;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  protected OCluster getCluster() {
    final ODatabaseDocumentInternal database = getDatabase();
    if (clusterId > -1) {
      return database.getStorage().getClusterById(clusterId);
    } else {
      return database.getStorage().getClusterById(database.getStorage().getClusterIdByName(clusterName));
    }
  }

  public String getSyntax() {
    return "ALTER CLUSTER <cluster-name>|<cluster-id> <attribute-name> <attribute-value>";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
