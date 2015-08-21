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
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateProperty extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_CREATE   = "CREATE";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  private String             className;
  private String             fieldName;
  private OType              type;
  private String             linked;
  private boolean            unsafe           = false;

  public OCommandExecutorSQLCreateProperty parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    final StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found", parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_PROPERTY + " not found", parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <class>.<property>", parserText, oldPos);

    String[] parts = split(word);
    if (parts.length != 2)
      throw new OCommandSQLParsingException("Expected <class>.<property>", parserText, oldPos);

    className = parts[0];
    if (className == null)
      throw new OCommandSQLParsingException("Class not found", parserText, oldPos);
    fieldName = parts[1];

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Missed property type", parserText, oldPos);

    type = OType.valueOf(word.toString());

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      return this;

    if (word.toString().equals(KEYWORD_UNSAFE))
      unsafe = true;
    else {
      linked = word.toString();

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1)
        return this;

      if (word.toString().equals(KEYWORD_UNSAFE))
        unsafe = true;
    }

    return this;
  }


  private String[] split(StringBuilder word) {
    List<String> result = new ArrayList<String>();
    StringBuilder builder = new StringBuilder();
    boolean quoted = false;
    for (char c : word.toString().toCharArray()) {
      if (!quoted) {
        if (c == '`') {
          quoted = true;
        } else if (c == '.') {
          String nextToken = builder.toString().trim();
          if (nextToken.length() > 0) {
            result.add(nextToken);
          }
          builder = new StringBuilder();
        } else {
          builder.append(c);
        }
      } else {
        if (c == '`') {
          quoted = false;
        } else {
          builder.append(c);
        }
      }
    }
    String nextToken = builder.toString().trim();
    if (nextToken.length() > 0) {
      result.add(nextToken);
    }
    return result.toArray(new String[] {});
    // return word.toString().split("\\.");
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  /**
   * Execute the CREATE PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (type == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseDocument database = getDatabase();
    final OClassImpl sourceClass = (OClassImpl) database.getMetadata().getSchema().getClass(className);
    if (sourceClass == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    OPropertyImpl prop = (OPropertyImpl) sourceClass.getProperty(fieldName);
    if (prop != null)
      throw new OCommandExecutionException("Property '" + className + "." + fieldName
          + "' already exists. Remove it before to retry.");

    // CREATE THE PROPERTY
    OClass linkedClass = null;
    OType linkedType = null;
    if (linked != null) {
      // FIRST SEARCH BETWEEN CLASSES
      linkedClass = database.getMetadata().getSchema().getClass(linked);

      if (linkedClass == null)
        // NOT FOUND: SEARCH BETWEEN TYPES
        linkedType = OType.valueOf(linked.toUpperCase(Locale.ENGLISH));
    }

    // CREATE IT LOCALLY
    sourceClass.addPropertyInternal(fieldName, type, linkedType, linkedClass, !unsafe);
    return sourceClass.properties().size();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  @Override
  public String getSyntax() {
    return "CREATE PROPERTY <class>.<property> <type> [<linked-type>|<linked-class>] [UNSAFE]";
  }
}
