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

/**
 * Base abstract class with RETRY
 * 
 * @author Luca Garulli
 */
public abstract class OCommandExecutorSQLRetryAbstract extends OCommandExecutorSQLSetAware {
  public static final String KEYWORD_RETRY = "RETRY";

  protected int              retry         = 1;
  protected int              wait          = 0;

  /**
   * Parses the RETRY number of times
   */
  protected void parseRetry() throws OCommandSQLParsingException {
    parserNextWord(true);
    retry = Integer.parseInt(parserGetLastWord());

    String temp = parseOptionalWord(true);

    if (temp.equals("WAIT")) {
      parserNextWord(true);
      wait = Integer.parseInt(parserGetLastWord());
    } else {
        parserGoBack();
    }
  }
}
