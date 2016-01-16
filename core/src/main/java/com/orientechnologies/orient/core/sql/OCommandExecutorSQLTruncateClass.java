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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * SQL TRUNCATE CLASS command: Truncates an entire class deleting all configured clusters where the class relies on.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLTruncateClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_TRUNCATE    = "TRUNCATE";
  public static final String KEYWORD_CLASS       = "CLASS";
  public static final String KEYWORD_POLYMORPHIC = "POLYMORPHIC";
  private OClass             schemaClass;
  private boolean            unsafe              = false;
  private boolean            deep                = false;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLTruncateClass parse(final OCommandRequest iRequest) {
    final ODatabaseDocument database = getDatabase();

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

    oldPos = pos;
    pos = nextWord(parserText, parserText, oldPos, word, true);

    while (pos > 0) {
      String nextWord = word.toString();
      if (nextWord.toUpperCase().equals(KEYWORD_UNSAFE)) {
        unsafe = true;
      } else if (nextWord.toUpperCase().equals(KEYWORD_POLYMORPHIC)) {
        deep = true;
      }
      oldPos = pos;
      pos = nextWord(parserText, parserText, oldPos, word, true);
    }

    return this;
  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (schemaClass == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final long recs = schemaClass.count();
    if (recs > 0 && !unsafe) {
      if (schemaClass.isSubClassOf("V")) {
        throw new OCommandExecutionException(
            "'TRUNCATE CLASS' command cannot be used on not empty vertex classes. Apply the 'UNSAFE' keyword to force it (at your own risk)");
      } else if (schemaClass.isSubClassOf("E")) {
        throw new OCommandExecutionException(
            "'TRUNCATE CLASS' command cannot be used on not empty edge classes. Apply the 'UNSAFE' keyword to force it (at your own risk)");
      }
    }

    Collection<OClass> subclasses = schemaClass.getAllSubclasses();
    if (deep && !unsafe) {// for multiple inheritance
      for (OClass subclass : subclasses) {
        long subclassRecs = schemaClass.count();
        if (subclassRecs > 0) {
          if (subclass.isSubClassOf("V")) {
            throw new OCommandExecutionException("'TRUNCATE CLASS' command cannot be used on not empty vertex classes ("
                + subclass.getName() + "). Apply the 'UNSAFE' keyword to force it (at your own risk)");
          } else if (subclass.isSubClassOf("E")) {
            throw new OCommandExecutionException("'TRUNCATE CLASS' command cannot be used on not empty edge classes ("
                + subclass.getName() + "). Apply the 'UNSAFE' keyword to force it (at your own risk)");
          }
        }
      }
    }

    try {
      schemaClass.truncate();
      if (deep) {
        for (OClass subclass : subclasses) {
          subclass.truncate();
        }
      }
    } catch (IOException e) {
      throw new OCommandExecutionException("Error on executing command", e);
    }

    return recs;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getSyntax() {
    return "TRUNCATE CLASS <class-name>";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }
}
