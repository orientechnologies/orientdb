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

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLGrant extends OCommandExecutorSQLPermissionAbstract {
  public static final String  KEYWORD_GRANT = "GRANT";
  private static final String KEYWORD_TO    = "TO";

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLGrant parse(final OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    privilege = ORole.PERMISSION_NONE;
    resource = null;
    role = null;

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_GRANT))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_GRANT + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Invalid privilege", parserText, oldPos);

    parsePrivilege(word, oldPos);

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_ON))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_ON + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserText, pos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Invalid resource", parserText, oldPos);

    resource = word.toString();

    pos = nextWord(parserText, parserTextUpperCase, pos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_TO))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_TO + " not found. Use " + getSyntax(), parserText, oldPos);

    pos = nextWord(parserText, parserText, pos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Invalid role", parserText, oldPos);

    final String roleName = word.toString();
    role = getDatabase().getMetadata().getSecurity().getRole(roleName);
    if (role == null)
      throw new OCommandSQLParsingException("Invalid role: " + roleName);
    return this;
  }

  /**
   * Execute the GRANT.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (role == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    role.grant(resource, privilege);
    role.save();

    return role;
  }

  public String getSyntax() {
    return "GRANT <permission> ON <resource> TO <role>";
  }
}
