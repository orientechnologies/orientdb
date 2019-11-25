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
package com.orientechnologies.agent.ha.sql;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SQL HA SET STATUS command: update the status of distributed servers and databases.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHASet extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String NAME        = "HA SET";
  public static final String KEYWORD_HA  = "HA";
  public static final String KEYWORD_SET = "SET";

  private static final Set<String> DBSTATUS_SUPPORTED = new HashSet<String>(Arrays.asList(
      new String[] { ODistributedServerManager.DB_STATUS.ONLINE.toString(),
          ODistributedServerManager.DB_STATUS.OFFLINE.toString() }));

  private String[] dbStatus;
  private String[] role;
  private String[] owner;

  public OCommandExecutorSQLHASet parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_HA))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_HA + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_SET))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_SET + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false, " \r\n");
    if (pos == -1)
      throw new OCommandSQLParsingException("Missing option. Use " + getSyntax(), parserText, oldPos);

    while (pos > -1) {
      final String option = word.toString();

      if (option.equalsIgnoreCase("DBSTATUS")) {
        pos = nextWord(parserText, parserTextUpperCase, pos, word, false, " \r\n");
        if (pos == -1)
          throw new OCommandSQLParsingException("Missing value. Use " + getSyntax(), parserText, oldPos);

        final String value = word.toString();

        dbStatus = value.split("=");
        if (dbStatus.length != 2)
          throw new OCommandSQLParsingException("Invalid format for value. Use " + getSyntax(), parserText, oldPos);

        dbStatus[1] = dbStatus[1].toUpperCase();

        // CHECK IF THE STATUS IS SUPPORTED
        if (!DBSTATUS_SUPPORTED.contains(dbStatus[1])) {
          throw new OCommandSQLParsingException("Invalid DBSTATUS '" + dbStatus[1] + "'. Supported are " + DBSTATUS_SUPPORTED,
              parserText, oldPos);
        }
      } else if (option.equalsIgnoreCase("ROLE")) {
        pos = nextWord(parserText, parserTextUpperCase, pos, word, false, " \r\n");
        if (pos == -1)
          throw new OCommandSQLParsingException("Missing value. Use " + getSyntax(), parserText, oldPos);

        final String value = word.toString();

        role = value.split("=");
        if (role.length != 2)
          throw new OCommandSQLParsingException("Invalid format for value. Use " + getSyntax(), parserText, oldPos);

        role[1] = role[1].toUpperCase();

        // CHECK IF THE ROLE IS SUPPORTED
        ODistributedConfiguration.ROLES.valueOf(role[1]);
      } else if (option.equalsIgnoreCase("OWNER")) {
        pos = nextWord(parserText, parserTextUpperCase, pos, word, false, " \r\n");
        if (pos == -1)
          throw new OCommandSQLParsingException("Missing value. Use " + getSyntax(), parserText, oldPos);

        final String value = word.toString();

        owner = value.split("=");
        if (owner.length != 2)
          throw new OCommandSQLParsingException("Invalid format for value. Use " + getSyntax(), parserText, oldPos);
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false, " \r\n");
    }

    if (dbStatus == null && role == null && owner == null)
      throw new OCommandSQLParsingException("Missing value. Use " + getSyntax(), parserText, oldPos);

    return this;

  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_UPDATE);

    if (!(database instanceof ODatabaseDocumentDistributed)) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }

    final OHazelcastPlugin dManager = (OHazelcastPlugin) ((ODatabaseDocumentDistributed) database).getStorageDistributed()
        .getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = database.getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);

    if (dbStatus != null) {
      dManager.setDatabaseStatus(dbStatus[0], databaseName, ODistributedServerManager.DB_STATUS.valueOf(dbStatus[1]));
    } else if (role != null) {
      final OModifiableDistributedConfiguration newCfg = cfg.modify();
      newCfg.setServerRole(role[0], ODistributedConfiguration.ROLES.valueOf(role[1]));
      dManager.updateCachedDatabaseConfiguration(databaseName, newCfg, true);
    } else if (owner != null) {
      final OModifiableDistributedConfiguration newCfg = cfg.modify();
      newCfg.setServerOwner(owner[0], owner[1]);
      dManager.updateCachedDatabaseConfiguration(databaseName, newCfg, true);
    } else
      throw new OCommandExecutionException("Invalid command HA SET");

    return Boolean.TRUE;
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }

  @Override
  public String getSyntax() {
    return "HA SET [DBSTATUS <server>=<status>] [ROLE <server>=MASTER|REPLICA] [OWNER <cluster>=<server>]";
  }
}
