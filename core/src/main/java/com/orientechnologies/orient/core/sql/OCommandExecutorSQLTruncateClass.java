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

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;

/**
 * SQL TRUNCATE CLASS command: Truncates an entire class deleting all configured clusters where the class relies on.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLTruncateClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_TRUNCATE = "TRUNCATE";
  public static final String KEYWORD_CLASS    = "CLASS";
  private OClass             schemaClass;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLTruncateClass parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();

    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_TRUNCATE + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserText, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected class name. Use " + getSyntax(), parserText, oldPos);

    final String className = word.toString();

    schemaClass = database.getMetadata().getSchema().getClass(className);

    if (schemaClass == null)
      throw new OCommandSQLParsingException("Class '" + className + "' not found", parserText, oldPos);
    return this;
  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (schemaClass == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final long recs = schemaClass.count();

    try {
      schemaClass.truncate();
    } catch (IOException e) {
      throw new OCommandExecutionException("Error on executing command", e);
    }

    return recs;
  }

  @Override
  public String getSyntax() {
    return "TRUNCATE CLASS <class-name>";
  }
}
