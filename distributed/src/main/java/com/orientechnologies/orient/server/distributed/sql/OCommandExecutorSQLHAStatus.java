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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODistributedOutput;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.Map;

/**
 * SQL HA STATUS command: returns the high availability configuration.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHAStatus extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String NAME           = "HA STATUS";
  public static final String KEYWORD_HA     = "HA";
  public static final String KEYWORD_STATUS = "STATUS";

  private boolean            servers        = false;
  private boolean            db             = false;
  private boolean            latency        = false;
  private boolean            messages = false;
  private boolean            textOutput     = false;

  public OCommandExecutorSQLHAStatus parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_HA))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_HA + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_STATUS))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_STATUS + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false, " \r\n");
    if (pos == -1)
      throw new OCommandSQLParsingException("Missing parameter. Use " + getSyntax(), parserText, oldPos);

    while (pos > -1) {
      final String option = word.toString();

      if (option.equalsIgnoreCase("-servers"))
        servers = true;
      else if (option.equalsIgnoreCase("-db"))
        db = true;
      else if (option.equalsIgnoreCase("-latency"))
        latency = true;
      else if (option.equalsIgnoreCase("-messages"))
        latency = true;
      else if (option.equalsIgnoreCase("-all"))
        servers = db = latency = messages = true;
      else if (option.equalsIgnoreCase("-output=text"))
        textOutput = true;

      pos = nextWord(parserText, parserTextUpperCase, pos, word, false, " \r\n");
    }

    return this;
  }

  /**
   * Execute the REMOVE SERVER command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_READ);

    final String dbUrl = database.getURL();

    final String path = dbUrl.substring(dbUrl.indexOf(":") + 1);
    final OServer serverInstance = OServer.getInstanceByPath(path);

    final OHazelcastPlugin dManager = (OHazelcastPlugin) serverInstance.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = database.getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);

    if (textOutput) {
      final StringBuilder output = new StringBuilder();
      if (servers)
        output.append(ODistributedOutput.formatServerStatus(dManager, dManager.getClusterConfiguration()));
      if (db)
        output.append(ODistributedOutput.formatClusterTable(dManager, databaseName, cfg, dManager.getAvailableNodes(databaseName)));
      if (latency)
        output.append(ODistributedOutput.formatLatency(dManager, dManager.getClusterConfiguration()));
      if (messages)
        output.append(ODistributedOutput.formatMessages(dManager, dManager.getClusterConfiguration()));
      return output.toString();
    }

    final ODocument output = new ODocument();
    if (servers)
      output.field("servers", dManager.getClusterConfiguration(), OType.EMBEDDED);
    if (db)
      output.field("database", cfg.getDocument(), OType.EMBEDDED);

    return output;
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
    return "HA STATUS [-servers] [-db] [-latency] [-messages] [-all] [-output=text]";
  }
}
