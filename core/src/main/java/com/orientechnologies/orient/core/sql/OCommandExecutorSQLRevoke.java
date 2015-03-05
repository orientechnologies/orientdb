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

import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL REVOKE command: Revoke a privilege to a database role.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLRevoke extends OCommandExecutorSQLPermissionAbstract {
  public static final String  KEYWORD_REVOKE = "REVOKE";
  private static final String KEYWORD_FROM   = "FROM";

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLRevoke parse(final OCommandRequest iRequest) {
    final ODatabaseDocument database = getDatabase();

    init((OCommandRequestText) iRequest);

    privilege = ORole.PERMISSION_NONE;
    resource = null;
    role = null;

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_REVOKE)) {
        throw new OCommandSQLParsingException("Keyword " + KEYWORD_REVOKE + " not found. Use " + getSyntax(), parserText, oldPos);
    }

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1) {
        throw new OCommandSQLParsingException("Invalid privilege", parserText, oldPos);
    }

    parsePrivilege(word, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_ON)) {
        throw new OCommandSQLParsingException("Keyword " + KEYWORD_ON + " not found. Use " + getSyntax(), parserText, oldPos);
    }

    pos = nextWord(parserText, parserText, pos, word, true);
    if (pos == -1) {
        throw new OCommandSQLParsingException("Invalid resource", parserText, oldPos);
    }

    resource = word.toString();

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_FROM)) {
        throw new OCommandSQLParsingException("Keyword " + KEYWORD_FROM + " not found. Use " + getSyntax(), parserText, oldPos);
    }

    pos = nextWord(parserText, parserText, pos, word, true);
    if (pos == -1) {
        throw new OCommandSQLParsingException("Invalid role", parserText, oldPos);
    }

    final String roleName = word.toString();
    role = database.getMetadata().getSecurity().getRole(roleName);
    if (role == null) {
        throw new OCommandSQLParsingException("Invalid role: " + roleName);
    }
    return this;
  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (role == null) {
        throw new OCommandExecutionException("Cannot execute the command because it has not yet been parsed");
    }

    role.revoke(resource, privilege);
    role.save();

    return role;
  }

  public String getSyntax() {
    return "REVOKE <permission> ON <resource> FROM <role>";
  }
}
