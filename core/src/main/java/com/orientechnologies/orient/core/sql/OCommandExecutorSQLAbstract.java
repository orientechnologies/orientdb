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

import com.orientechnologies.orient.core.command.OCommandContext.TIMEOUT_STRATEGY;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * SQL abstract Command Executor implementation.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OCommandExecutorSQLAbstract extends OCommandExecutorAbstract {

  public static final String KEYWORD_FROM      = "FROM";
  public static final String KEYWORD_LET       = "LET";
  public static final String KEYWORD_WHERE     = "WHERE";
  public static final String KEYWORD_LIMIT     = "LIMIT";
  public static final String KEYWORD_SKIP      = "SKIP";
  public static final String KEYWORD_OFFSET    = "OFFSET";
  public static final String KEYWORD_TIMEOUT   = "TIMEOUT";
  public static final String KEYWORD_KEY       = "key";
  public static final String KEYWORD_RID       = "rid";
  public static final String CLUSTER_PREFIX    = "CLUSTER:";
  public static final String CLASS_PREFIX      = "CLASS:";
  public static final String INDEX_PREFIX      = "INDEX:";
  public static final String DICTIONARY_PREFIX = "DICTIONARY:";
  public static final String METADATA_PREFIX   = "METADATA:";
  public static final String METADATA_SCHEMA   = "SCHEMA";
  public static final String METADATA_INDEXMGR = "INDEXMANAGER";

  protected long             timeoutMs         = OGlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();
  protected TIMEOUT_STRATEGY timeoutStrategy   = TIMEOUT_STRATEGY.EXCEPTION;

  protected void throwSyntaxErrorException(final String iText) {
    throw new OCommandSQLParsingException(iText + ". Use " + getSyntax(), parserText, parserGetPreviousPosition());
  }

  protected void throwParsingException(final String iText) {
    throw new OCommandSQLParsingException(iText, parserText, parserGetPreviousPosition());
  }

  /**
   * The command is replicated
   * 
   * @return
   */
  public boolean isReplicated() {
    return true;
  }

  public boolean isIdempotent() {
    return false;
  }

  /**
   * Parses the timeout keyword if found.
   */
  protected boolean parseTimeout(final String w) throws OCommandSQLParsingException {
    if (!w.equals(KEYWORD_TIMEOUT))
      return false;

    parserNextWord(true);
    String word = parserGetLastWord();

    try {
      timeoutMs = Long.parseLong(word);
    } catch (Exception e) {
      throwParsingException("Invalid " + KEYWORD_TIMEOUT + " value setted to '" + word
          + "' but it should be a valid long. Example: " + KEYWORD_TIMEOUT + " 3000");
    }

    if (timeoutMs < 0)
      throwParsingException("Invalid " + KEYWORD_TIMEOUT + ": value setted to less than ZERO. Example: " + timeoutMs + " 10");

    parserNextWord(true);
    word = parserGetLastWord();

    if (word.equals(TIMEOUT_STRATEGY.EXCEPTION.toString()))
      timeoutStrategy = TIMEOUT_STRATEGY.EXCEPTION;
    else if (word.equals(TIMEOUT_STRATEGY.RETURN.toString()))
      timeoutStrategy = TIMEOUT_STRATEGY.RETURN;
    else
      parserGoBack();

    return true;
  }
}
