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
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OHaStatusStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedOutput;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import java.util.Map;

/**
 * SQL HA STATUS command: returns the high availability configuration.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHAStatus extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String NAME = "HA STATUS";
  public static final String KEYWORD_HA = "HA";
  public static final String KEYWORD_STATUS = "STATUS";

  private OHaStatusStatement parsedStatement;

  public OCommandExecutorSQLHAStatus parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);
    try {
      parsedStatement = (OHaStatusStatement) OStatementCache.get(this.parserText, getDatabase());
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
    database.checkSecurity(ORule.ResourceGeneric.SERVER, "status", ORole.PERMISSION_READ);

    if (!(database instanceof ODatabaseDocumentDistributed)) {
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");
    }

    final ODistributedPlugin dManager =
        (ODistributedPlugin) ((ODatabaseDocumentDistributed) database).getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = database.getName();

    final ODistributedConfiguration cfg = dManager.getDatabaseConfiguration(databaseName);

    if (parsedStatement.outputText) {
      final StringBuilder output = new StringBuilder();
      if (parsedStatement.servers)
        output.append(
            ODistributedOutput.formatServerStatus(dManager, dManager.getClusterConfiguration()));
      if (parsedStatement.db)
        output.append(
            ODistributedOutput.formatClusterTable(
                dManager, databaseName, cfg, dManager.getTotalNodes(databaseName)));
      if (parsedStatement.latency)
        output.append(
            ODistributedOutput.formatLatency(dManager, dManager.getClusterConfiguration()));
      if (parsedStatement.messages)
        output.append(
            ODistributedOutput.formatMessages(dManager, dManager.getClusterConfiguration()));
      if (parsedStatement.locks) {
        output.append(ODistributedOutput.formatNewRecordLocks(dManager, databaseName));
      }
      return output.toString();
    }

    final ODocument output = new ODocument();
    if (parsedStatement.servers)
      output.field("servers", dManager.getClusterConfiguration(), OType.EMBEDDED);
    if (parsedStatement.db) output.field("database", cfg.getDocument(), OType.EMBEDDED);

    if (parsedStatement.locks) {
      output.field("locks", ODistributedOutput.getRequestsStatus(dManager, databaseName));
    }

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

  @Override
  public boolean isIdempotent() {
    return true;
  }
}
