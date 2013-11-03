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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL ALTER DATABASE command: Changes an attribute of the current database.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterDatabase extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String   KEYWORD_ALTER    = "ALTER";
  public static final String   KEYWORD_DATABASE = "DATABASE";

  private ODatabase.ATTRIBUTES attribute;
  private String               value;

  public OCommandExecutorSQLAlterDatabase parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_DATABASE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_DATABASE + " not found. Use " + getSyntax(), parserText, oldPos);

    oldPos = pos;
    pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Missed the database's attribute to change. Use " + getSyntax(), parserText, oldPos);

    final String attributeAsString = word.toString();

    try {
      attribute = ODatabase.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new OCommandSQLParsingException("Unknown database's attribute '" + attributeAsString + "'. Supported attributes are: "
          + Arrays.toString(ODatabase.ATTRIBUTES.values()), parserText, oldPos);
    }

    value = parserText.substring(pos + 1).trim();

    if (value.length() == 0)
      throw new OCommandSQLParsingException("Missed the database's value to change for attribute '" + attribute + "'. Use "
          + getSyntax(), parserText, oldPos);

    if (value.equalsIgnoreCase("null"))
      value = null;

    return this;
  }

  /**
   * Execute the ALTER DATABASE.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (attribute == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);

    ((ODatabaseComplex<?>) database).setInternal(attribute, value);
    return null;
  }

  public String getSyntax() {
    return "ALTER DATABASE <attribute-name> <attribute-value>";
  }
}
