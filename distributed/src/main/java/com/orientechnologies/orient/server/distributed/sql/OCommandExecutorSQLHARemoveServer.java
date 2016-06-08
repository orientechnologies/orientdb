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
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.Map;

/**
 * SQL HA REMOVE SERVER command: removes a server from ha configuration.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHARemoveServer extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String NAME           = "HA REMOVE SERVER";
  public static final String KEYWORD_HA     = "HA";
  public static final String KEYWORD_REMOVE = "REMOVE";
  public static final String KEYWORD_SERVER = "SERVER";

  private String             serverName;

  public OCommandExecutorSQLHARemoveServer parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_HA))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_HA + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_REMOVE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_REMOVE + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_SERVER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_SERVER + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <server-name>. Use " + getSyntax(), parserText, pos);

    serverName = word.toString();
    if (serverName == null)
      throw new OCommandSQLParsingException("Server name is null. Use " + getSyntax(), parserText, pos);

    return this;
  }

  /**
   * Execute the REMOVE SERVER command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE);

    final String dbUrl = database.getURL();

    final String path = dbUrl.substring(dbUrl.indexOf(":") + 1);
    final OServer serverInstance = OServer.getInstanceByPath(path);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) serverInstance.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = database.getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);
    if (cfg.removeServer(serverName) != null) {
      // SERVER REMOVED CORRECTLY
      dManager.updateCachedDatabaseConfiguration(databaseName, cfg.getDocument(), true, true);
      return true;
    }

    // PUT THE DATABASE OFFLINE FOR THAT SERVER
    dManager.setDatabaseStatus(serverName, databaseName, ODistributedServerManager.DB_STATUS.OFFLINE);

    return false;
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
