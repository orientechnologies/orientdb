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

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;

/**
 * SQL REMOVE INDEX command: Remove an index
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLRebuildIndex extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_REBUILD = "REBUILD";
  public static final String KEYWORD_INDEX   = "INDEX";

  private String             name;

  public OCommandExecutorSQLRebuildIndex parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_REBUILD))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_REBUILD + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_INDEX))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_INDEX + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected index name", parserText, oldPos);

    name = word.toString();

    return this;
  }

  /**
   * Execute the REMOVE INDEX.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (name == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    if (name.equals("*")) {
      long totalIndexed = 0;
      for (OIndex<?> idx : database.getMetadata().getIndexManager().getIndexes()) {
        if (idx.isAutomatic())
          totalIndexed += idx.rebuild();
      }

      return totalIndexed;

    } else {
      final OIndex<?> idx = database.getMetadata().getIndexManager().getIndex(name);
      if (idx == null)
        throw new OCommandExecutionException("Index '" + name + "' not found");

      if (!idx.isAutomatic())
        throw new OCommandExecutionException("Cannot rebuild index '" + name
            + "' because it's manual and there aren't indications of what to index");

      return idx.rebuild();
    }
  }

  @Override
  public String getSyntax() {
    return "REBUILD INDEX <index-name>";
  }
}
