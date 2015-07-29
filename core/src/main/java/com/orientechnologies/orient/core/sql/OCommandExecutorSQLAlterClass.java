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
import com.orientechnologies.orient.core.metadata.schema.OClass.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterClass extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_ALTER = "ALTER";
  public static final String KEYWORD_CLASS = "CLASS";

  private String             className;
  private ATTRIBUTES         attribute;
  private String             value;
  private boolean            unsafe        = false;

  public OCommandExecutorSQLAlterClass parse(final OCommandRequest iRequest) {
    final ODatabaseDocument database = getDatabase();

    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found", parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found", parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected <class>", parserText, oldPos);

    className = word.toString();

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Missed the class's attribute to change", parserText, oldPos);

    final String attributeAsString = word.toString();

    try {
      attribute = OClass.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new OCommandSQLParsingException("Unknown class's attribute '" + attributeAsString + "'. Supported attributes are: "
          + Arrays.toString(OClass.ATTRIBUTES.values()), parserText, oldPos, e);
    }

    value = parserText.substring(pos + 1).trim();

    if (parserTextUpperCase.endsWith("UNSAFE")) {
      unsafe = true;
      value = value.substring(0, value.length() - "UNSAFE".length());
      for (int i = value.length() - 1; value.charAt(i) == ' ' || value.charAt(i) == '\t'; i--)
        value = value.substring(0, value.length() - 1);
    }
    if (value.length() == 0)
      throw new OCommandSQLParsingException("Missed the property's value to change for attribute '" + attribute + "'", parserText,
          oldPos);

    if (value.equalsIgnoreCase("null"))
      value = null;

    return this;
  }

  /**
   * Execute the ALTER CLASS.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    final ODatabaseDocument database = getDatabase();

    if (attribute == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final OClassImpl cls = (OClassImpl) database.getMetadata().getSchema().getClass(className);
    if (cls == null)
      throw new OCommandExecutionException("Cannot alter class '" + className + "' because not found");

    if (!unsafe && attribute == ATTRIBUTES.NAME && cls.isSubClassOf("E"))
      throw new OCommandExecutionException("Cannot alter class '" + className
          + "' because is an Edge class and could break vertices. Use UNSAFE if you want to force it");

    if (value != null && attribute == ATTRIBUTES.SUPERCLASS) {
      checkClassExists(database, className, value);
    }
    if (value != null && attribute == ATTRIBUTES.SUPERCLASSES) {
      List<String> classes = Arrays.asList(value.split(",\\s*"));
      for (String cName : classes) {
        checkClassExists(database, className, cName);
      }
    }
    if (!unsafe && value != null && attribute == ATTRIBUTES.NAME) {
      if(!cls.getIndexes().isEmpty()){
        throw new OCommandExecutionException("Cannot rename class '" + className
            + "' because it has indexes defined on it. Drop indexes before or use UNSAFE (at your won risk)");
      }
    }
    cls.set(attribute, value);

    return null;
  }

  @Override
  public long getTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  protected void checkClassExists(ODatabaseDocument database, String targetClass, String superClass) {
    if (superClass.startsWith("+") || superClass.startsWith("-")) {
      superClass = superClass.substring(1);
    }
    if (database.getMetadata().getSchema().getClass(superClass) == null) {
      throw new OCommandExecutionException("Cannot alter superClass of '" + targetClass + "' because  " + superClass
          + " class not found");
    }
  }

  public String getSyntax() {
    return "ALTER CLASS <class> <attribute-name> <attribute-value> [UNSAFE]";
  }

  @Override
  public boolean involveSchema() {
    return true;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
