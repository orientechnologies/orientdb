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
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterProperty extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_ALTER    = "ALTER";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  private String             className;
  private String             fieldName;
  private ATTRIBUTES         attribute;
  private String             value;

  public OCommandExecutorSQLAlterProperty parse(final OCommandRequest iRequest) {

    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_PROPERTY + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <class>.<property>. Use " + getSyntax(), parserText, oldPos);

    String[] parts = word.toString().split("\\.");
    if (parts.length != 2)
      throw new OCommandSQLParsingException("Expected <class>.<property>. Use " + getSyntax(), parserText, oldPos);

    className = parts[0];
    if (className == null)
      throw new OCommandSQLParsingException("Class not found", parserText, oldPos);
    fieldName = parts[1];

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Missing property attribute to change. Use " + getSyntax(), parserText, oldPos);

    final String attributeAsString = word.toString();

    try {
      attribute = OProperty.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new OCommandSQLParsingException("Unknown property attribute '" + attributeAsString + "'. Supported attributes are: "
          + Arrays.toString(OProperty.ATTRIBUTES.values()), parserText, oldPos, e);
    }

    value = parserText.substring(pos + 1).trim();

    if (value.length() == 0)
      throw new OCommandSQLParsingException("Missing property value to change for attribute '" + attribute + "'. Use "
          + getSyntax(), parserText, oldPos);

    if (value.equalsIgnoreCase("null"))
      value = null;

    return this;
  }

  @Override
  public long getTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  /**
   * Execute the ALTER PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (attribute == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not yet been parsed");

    final OClassImpl sourceClass = (OClassImpl) getDatabase().getMetadata().getSchema().getClass(className);
    if (sourceClass == null)
      throw new OCommandExecutionException("Source class '" + className + "' not found");

    final OPropertyImpl prop = (OPropertyImpl) sourceClass.getProperty(fieldName);
    if (prop == null)
      throw new OCommandExecutionException("Property '" + className + "." + fieldName + "' not exists");

    prop.set(attribute, value);
    return null;
  }

  public String getSyntax() {
    return "ALTER PROPERTY <class>.<property> <attribute-name> <attribute-value>";
  }
}
