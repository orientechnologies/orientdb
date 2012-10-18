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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL CREATE FUNCTION command.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLCreateFunction extends OCommandExecutorSQLAbstract {
  public static final String NAME       = "CREATE FUNCTION";
  private String             name;
  private String             code;
  private String             language;
  private boolean            idempotent = false;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLCreateFunction parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    init(((OCommandRequestText) iRequest).getText());

    parserRequiredKeyword("CREATE");
    parserRequiredKeyword("FUNCTION");

    parserNextWord(false);
    name = parserGetLastWord();
    parserNextWord(false);
    code = parserGetLastWord();

    String temp = parseOptionalWord(true);
    while (temp != null) {
      if (temp.equals("IDEMPOTENT")) {
        parserNextWord(false);
        idempotent = Boolean.parseBoolean(parserGetLastWord());
      } else if (temp.equals("LANGUAGE")) {
        parserNextWord(false);
        language = parserGetLastWord();
      }

      temp = parserOptionalWord(true);
      if (parserIsEnded())
        break;
    }
    return this;
  }

  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (name == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    ODatabaseRecord database = getDatabase();
    final OFunction f = database.getMetadata().getFunctionLibrary().createFunction(name);
    f.setCode(code);
    f.setIdempotent(idempotent);
    if (language != null)
      f.setLanguage(language);

    return f.getId();
  }

  @Override
  public String getSyntax() {
    return "CREATE FUNCTION <name> <code> [IDEMPOTENT true|false] [LANGUAGE <language>]";
  }

}
