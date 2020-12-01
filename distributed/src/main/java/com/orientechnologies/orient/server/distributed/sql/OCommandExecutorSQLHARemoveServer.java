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
package com.orientechnologies.orient.server.distributed.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OHaRemoveServerStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import java.util.Map;

/**
 * SQL HA REMOVE SERVER command: removes a server from ha configuration.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHARemoveServer extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String NAME = "HA REMOVE SERVER";

  private OHaRemoveServerStatement parsedStatement;

  public OCommandExecutorSQLHARemoveServer parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);
    try {
      parsedStatement =
          (OHaRemoveServerStatement) OStatementCache.get(this.parserText, getDatabase());
      preParsedStatement = parsedStatement;
    } catch (OCommandSQLParsingException sqlx) {
      throw sqlx;
    } catch (Exception e) {
      throwParsingException("Error parsing query: \n" + this.parserText + "\n" + e.getMessage(), e);
    }
    return this;
  }

  /** Execute the command. */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE);

    if (!(database instanceof ODatabaseDocumentDistributed)) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }

    final ODistributedPlugin dManager =
        (ODistributedPlugin) ((ODatabaseDocumentDistributed) database).getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = database.getName();

    // The last parameter (true) indicates to set the node's database status to OFFLINE.
    // If this is changed to false, the node will be set to NOT_AVAILABLE, and then the
    // auto-repairer will
    // re-synchronize the database on the node, and then set it to ONLINE.
    return dManager.removeNodeFromConfiguration(
        parsedStatement.serverName.getStringValue(), databaseName, false, true);
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }

  @Override
  public String getSyntax() {
    return "HA REMOVE SERVER <server-name>";
  }
}
