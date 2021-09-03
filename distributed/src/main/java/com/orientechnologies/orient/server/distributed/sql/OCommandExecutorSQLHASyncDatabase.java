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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.parser.OHaSyncDatabaseStatement;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import java.util.Map;

/**
 * SQL HA SYNC DATABASE command: synchronizes database form distributed servers.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLHASyncDatabase extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {
  public static final String NAME = "HA SYNC DATABASE";
  private OHaSyncDatabaseStatement parsedStatement;

  public OCommandExecutorSQLHASyncDatabase parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    try {
      parsedStatement =
          (OHaSyncDatabaseStatement) OStatementCache.get(this.parserText, getDatabase());
      preParsedStatement = parsedStatement;
    } catch (OCommandSQLParsingException sqlx) {
      throw sqlx;
    } catch (Exception e) {
      throwParsingException("Error parsing query: \n" + this.parserText + "\n" + e.getMessage(), e);
    }

    return this;
  }

  /** Execute the SYNC DATABASE. */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    return database.sync(parsedStatement.isForce(), !parsedStatement.isFull());
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "HA SYNC DATABASE [-force] [-full]";
  }
}
