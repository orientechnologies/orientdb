/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.orient.core.exception.OCoreException;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.sql.parser.TokenMgrError;

public class OCommandSQLParsingException extends OCoreException {

  private Integer line;
  private Integer column;
  private String statement;
  private String text;
  private int position;
  private static final long serialVersionUID = -7430575036316163711L;

  public OCommandSQLParsingException(ParseException e, String statement) {
    super(
        generateMessage(e, statement, e.currentToken.next.beginLine, e.currentToken.next.endColumn),
        null,
        OErrorCode.QUERY_PARSE_ERROR);
    this.statement = statement;
    this.line = e.currentToken.next.beginLine;
    this.column = e.currentToken.next.endColumn;
  }

  public OCommandSQLParsingException(TokenMgrError e, String statement) {
    super(e.getMessage(), null, OErrorCode.QUERY_PARSE_ERROR);
    this.statement = statement;
    this.line = 0;
    this.column = 0;
  }

  private static String generateMessage(
      ParseException e, String statement, Integer line, Integer column) {
    StringBuilder result = new StringBuilder();
    result.append("Error parsing query:\n");
    String[] stmLines = statement.split("\n");
    for (int i = 0; i < stmLines.length; i++) {
      result.append(stmLines[i]);
      result.append("\n");
      if (i == line - 1) {
        for (int c = 0; c < column - 1; c++) {
          result.append(' ');
        }
        result.append("^\n");
      }
    }
    result.append(e.getMessage());
    return result.toString();
  }

  private static String makeMessage(int position, String text, String message) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("Error on parsing command");
    buffer.append(": ").append(message);

    if (text != null) {
      buffer.append("\nCommand: ");
      buffer.append(text);
      buffer.append("\n---------");
      for (int i = 0; i < position - 1; ++i) buffer.append("-");

      buffer.append("^");
    }
    return buffer.toString();
  }

  public OCommandSQLParsingException(OCommandSQLParsingException exception) {
    super(exception);

    this.text = exception.text;
    this.position = exception.position;
  }

  public OCommandSQLParsingException(String iMessage) {
    super(iMessage);
  }

  public OCommandSQLParsingException(String iMessage, String iText, int iPosition) {
    super(makeMessage(iPosition, iText, iMessage));

    text = iText;
    position = iPosition;
  }

  public Integer getLine() {
    return line;
  }

  public Integer getColumn() {
    return column;
  }

  public String getStatement() {
    return statement;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) return false;

    return toString().equals(obj.toString());
  }
}
