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
import com.orientechnologies.orient.core.exception.OCommandExecutionException;

/**
 * SQL REMOVE INDEX command: Remove an index
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropIndex extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_DROP  = "DROP";
  public static final String KEYWORD_INDEX = "INDEX";

  private String             name;

  public OCommandExecutorSQLDropIndex parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_INDEX))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_INDEX + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected index name. Use " + getSyntax(), parserText, oldPos);

    name = word.toString();

    return this;
  }

  /**
   * Execute the REMOVE INDEX.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (name == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    getDatabase().getMetadata().getIndexManager().dropIndex(name);
    return null;
  }

  @Override
  public String getSyntax() {
    return "DROP INDEX <index-name>|<class>.<property>";
  }
}
