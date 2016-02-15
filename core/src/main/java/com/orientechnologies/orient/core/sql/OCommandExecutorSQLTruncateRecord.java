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
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import com.orientechnologies.orient.core.version.OVersionFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SQL TRUNCATE RECORD command: Truncates a record without loading it. Useful when the record is dirty in any way and cannot be
 * loaded correctly.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLTruncateRecord extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_TRUNCATE = "TRUNCATE";
  public static final String KEYWORD_RECORD   = "RECORD";
  private Set<String>        records          = new HashSet<String>();

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLTruncateRecord parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_TRUNCATE + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_RECORD))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_RECORD + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserText, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected one or more records. Use " + getSyntax(), parserText, oldPos);

    if (word.charAt(0) == '[')
      // COLLECTION
      OStringSerializerHelper.getCollection(parserText, oldPos, records);
    else {
      records.add(word.toString());
    }

    if (records.isEmpty())
      throw new OCommandSQLParsingException("Missed record(s). Use " + getSyntax(), parserText, oldPos);
    return this;
  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (records.isEmpty())
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    int deleted = 0;

    final ODatabaseDocumentInternal database = getDatabase();
    for (String rec : records) {
      try {
        final ORecordId rid = new ORecordId(rec);
        final OStorageOperationResult<Boolean> result = database.getStorage().deleteRecord(rid,
            OVersionFactory.instance().createUntrackedVersion(), 0, null);
        database.getLocalCache().deleteRecord(rid);

        if (result.getResult())
          deleted++;

      } catch (Throwable e) {
        throw new OCommandExecutionException("Error on executing command", e);
      }
    }

    return deleted;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getSyntax() {
    return "TRUNCATE RECORD <rid>*";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }
}
