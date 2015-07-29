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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLAbstract;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedStorage;
import com.orientechnologies.orient.server.distributed.task.OSyncDatabaseTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.Map;

/**
 * SQL SYNC DATABASE command: synchronize database form distributed servers.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLSyncDatabase extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String     NAME             = "SYNC DATABASE";
  public static final String     KEYWORD_SYNC     = "SYNC";
  public static final String     KEYWORD_DATABASE = "DATABASE";

  private OSyncDatabaseTask.MODE mode             = OSyncDatabaseTask.MODE.FULL_REPLACE;

  public OCommandExecutorSQLSyncDatabase parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_SYNC))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_SYNC + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_DATABASE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_DATABASE + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, false);
    if (pos != -1) {
      mode = OSyncDatabaseTask.MODE.valueOf(word.toString());
    }

    return this;
  }

  /**
   * Execute the SYNC DATABASE.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocumentInternal database = getDatabase();
    database.checkSecurity(ORule.ResourceGeneric.DATABASE, "sync", ORole.PERMISSION_UPDATE);

    final OStorage stg = database.getStorage();
    if (!(stg instanceof ODistributedStorage))
      throw new ODistributedException("SYNC DATABASE command cannot be executed against a non distributed server");

    final ODistributedStorage dStg = (ODistributedStorage) stg;

    final OHazelcastPlugin dManager = (OHazelcastPlugin) dStg.getDistributedManager();
    if (dManager == null || !dManager.isEnabled())
      throw new OCommandExecutionException("OrientDB is not started in distributed mode");

    final String databaseName = database.getName();

    Map<String, Object> config = dManager.getConfigurationMap();
    final ODocument dbConf = (ODocument) config.get(OHazelcastPlugin.CONFIG_DATABASE_PREFIX + databaseName);

    return dManager.installDatabase(true, databaseName, dbConf);
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public long getTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "SYNC DATABASE";
  }
}
