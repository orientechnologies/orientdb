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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * SQL CREATE CLUSTER command: Creates a new cluster.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateCluster extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE      = "CREATE";
  public static final String KEYWORD_CLUSTER     = "CLUSTER";
  public static final String KEYWORD_ID          = "ID";
  public static final String KEYWORD_DATASEGMENT = "DATASEGMENT";
  public static final String KEYWORD_LOCATION    = "LOCATION";
  public static final String KEYWORD_POSITION    = "POSITION";

  private String             clusterName;
  private String             clusterType;
  private int                requestedId         = -1;
  private String             dataSegmentName     = "default";
  private String             location            = "default";
  private String             position            = "append";

  public OCommandExecutorSQLCreateCluster parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();

    init((OCommandRequestText) iRequest);

    parserRequiredKeyword(KEYWORD_CREATE);
    parserRequiredKeyword(KEYWORD_CLUSTER);

    clusterName = parserRequiredWord(false);
    if (!clusterName.isEmpty() && Character.isDigit(clusterName.charAt(0)))
      throw new IllegalArgumentException("Cluster name cannot begin with a digit");

    clusterType = parserRequiredWord(false);

    String temp = parseOptionalWord(true);

    while (temp != null) {
      if (temp.equals(KEYWORD_ID)) {
        requestedId = Integer.parseInt(parserRequiredWord(false));

      } else if (temp.equals(KEYWORD_DATASEGMENT)) {
        dataSegmentName = parserRequiredWord(false);

      } else if (temp.equals(KEYWORD_LOCATION)) {
        location = parserRequiredWord(false);

      } else if (temp.equals(KEYWORD_POSITION)) {
        position = parserRequiredWord(false);

      }

      temp = parseOptionalWord(true);
      if (parserIsEnded())
        break;
    }

    final int clusterId = database.getStorage().getClusterIdByName(clusterName);
    if (clusterId > -1)
      throw new OCommandSQLParsingException("Cluster '" + clusterName + "' already exists");

    if (!(database.getStorage() instanceof OLocalPaginatedStorage)) {
      final int dataId = database.getStorage().getDataSegmentIdByName(dataSegmentName);
      if (dataId == -1)
        throw new OCommandSQLParsingException("Data segment '" + dataSegmentName + "' does not exists");
    }

    if (!Orient.instance().getClusterFactory().isSupported(clusterType))
      throw new OCommandSQLParsingException("Cluster type '" + clusterType + "' is not supported");

    return this;
  }

  /**
   * Execute the CREATE CLUSTER.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clusterName == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();

    if (requestedId == -1) {
      return database.addCluster(clusterType, clusterName, location, dataSegmentName);
    } else {
      return database.addCluster(clusterType, clusterName, requestedId, location, dataSegmentName);
    }
  }

  @Override
  public String getSyntax() {
    return "CREATE CLUSTER <name> <type> [DATASEGMENT <data-segment>|default] [LOCATION <path>|default] [POSITION <position>|append]";
  }
}
