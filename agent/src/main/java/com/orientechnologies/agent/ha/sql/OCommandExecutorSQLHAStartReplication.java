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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;
import com.orientechnologies.orient.server.distributed.impl.task.OStartReplicationTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQL HA START REPLICATION command: starts the replication against a server. The command is executed only on local server.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHAStartReplication extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String NAME                = "HA START REPLICATION";
  public static final String KEYWORD_HA          = "HA";
  public static final String KEYWORD_START       = "START";
  public static final String KEYWORD_REPLICATION = "REPLICATION";

  enum MODE {
    FULL, DELTA
  }

  private MODE   mode = MODE.DELTA;
  private String server;

  public OCommandExecutorSQLHAStartReplication parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_HA))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_HA + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_START))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_START + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_REPLICATION))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_REPLICATION + " not found. Use " + getSyntax(), parserText,
          oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Missing server. Use " + getSyntax(), parserText, oldPos);

    server = word.toString();
    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);

    while (pos > -1) {
      final String option = word.toString();

      if (option.equalsIgnoreCase("-mode")) {
        pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
        if (pos == -1)
          throw new OCommandSQLParsingException("Missing mode. Use " + getSyntax(), parserText, oldPos);

        try {
          mode = MODE.valueOf(word.toString().toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new OCommandSQLParsingException("Invalid mode. Use " + getSyntax(), parserText, oldPos);
        }
      }

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    }

    final ODatabaseDocumentInternal database = getDatabase();
    final OStorage stg = database.getStorage();
    if (!(stg instanceof ODistributedStorage))
      throw new ODistributedException("HA START REPLICATION command cannot be executed against a non distributed server");

    final ODistributedStorage dStg = (ODistributedStorage) stg;

    final OHazelcastPlugin dManager = (OHazelcastPlugin) dStg.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    if (!dManager.getActiveServers().contains(server))
      throw new OCommandExecutionException("Server '" + server + "' is not running");

    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.DATABASE, "sync", ORole.PERMISSION_UPDATE);

    final OStorage stg = database.getStorage();
    if (!(stg instanceof ODistributedStorage))
      throw new ODistributedException("HA START REPLICATION command cannot be executed against a non distributed server");

    final ODistributedStorage dStg = (ODistributedStorage) stg;

    final OHazelcastPlugin dManager = (OHazelcastPlugin) dStg.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final List<String> servers = new ArrayList<String>();
    servers.add(server);

    return dManager.sendRequest(database.getName(), null, servers,
        new OStartReplicationTask(database.getName(), mode == MODE.DELTA), dManager.getNextMessageIdCounter(),
        ODistributedRequest.EXECUTION_MODE.NO_RESPONSE, null, null);
  }

  /**
   * If server is specified, executes the command on the target server.
   */
  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "HA START REPLICATION <server> [-mode <FULL|DELTA>]";
  }
}
