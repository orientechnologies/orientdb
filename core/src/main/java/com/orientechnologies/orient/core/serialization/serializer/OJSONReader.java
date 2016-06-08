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
package com.orientechnologies.orient.core.serialization.serializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.Arrays;

public class OJSONReader {
  public static final char   NEW_LINE          = '\n';
  public static final char[] DEFAULT_JUMP      = new char[] { ' ', '\r', '\n', '\t' };
  public static final char[] DEFAULT_SKIP      = new char[] { '\r', '\n', '\t' };
  public static final char[] BEGIN_OBJECT      = new char[] { '{' };
  public static final char[] END_OBJECT        = new char[] { '}' };
  public static final char[] FIELD_ASSIGNMENT  = new char[] { ':' };
  public static final char[] BEGIN_STRING      = new char[] { '"' };
  public static final char[] COMMA_SEPARATOR   = new char[] { ',' };
  public static final char[] NEXT_IN_OBJECT    = new char[] { ',', '}' };
  public static final char[] NEXT_IN_ARRAY     = new char[] { ',', ']' };
  public static final char[] NEXT_OBJ_IN_ARRAY = new char[] { '{', ']' };
  public static final char[] ANY_NUMBER        = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
  public static final char[] BEGIN_COLLECTION  = new char[] { '[' };
  public static final char[] END_COLLECTION    = new char[] { ']' };
  private BufferedReader     in;
  private int                cursor            = 0;
  private int                lineNumber        = 0;
  private int                columnNumber      = 0;
  private StringBuilder      buffer            = new StringBuilder(16384);                                       // 16KB
  private String             value;
  private char               c;
  private char               lastCharacter;
  private Character          missedChar;

  public OJSONReader(Reader iIn) {
    this.in = new BufferedReader(iIn);
  }

  public int getCursor() {
    return cursor;
  }

  public OJSONReader checkContent(final String iExpected) throws ParseException {
    if (!value.equals(iExpected))
      throw new ParseException("Expected content is " + iExpected + " but found " + value, cursor);
    return this;
  }

  public boolean isContent(final String iExpected) {
    return value.equals(iExpected);
  }

  public int readInteger(final char[] iUntil) throws IOException, ParseException {
    return readNumber(iUntil, false);
  }

  public int readNumber(final char[] iUntil, final boolean iInclude) throws IOException, ParseException {
    if (readNext(iUntil, iInclude) == null)
      throw new ParseException("Expected integer", cursor);

    return Integer.parseInt(value.trim());
  }

  public String readString(final char[] iUntil) throws IOException, ParseException {
    return readString(iUntil, false);
  }

  public String readString(final char[] iUntil, final boolean iInclude) throws IOException, ParseException {
    return readString(iUntil, iInclude, DEFAULT_JUMP, null);
  }

  public String readString(final char[] iUntil, final boolean iInclude, final char[] iJumpChars, final char[] iSkipChars)
      throws IOException, ParseException {
    if (readNext(iUntil, iInclude, iJumpChars, iSkipChars) == null)
      return null;

    if (!iInclude && value.startsWith("\"")) {
      return value.substring(1, value.lastIndexOf("\""));
    }

    return value;
  }

  public String readString(final char[] iUntil, final boolean iInclude, final char[] iJumpChars, final char[] iSkipChars, boolean preserveQuotes)
      throws IOException, ParseException {
    if (readNext(iUntil, iInclude, iJumpChars, iSkipChars, preserveQuotes) == null)
      return null;

    if (!iInclude && value.startsWith("\"")) {
      return value.substring(1, value.lastIndexOf("\""));
    }

    return value;
  }


  public boolean readBoolean(final char[] nextInObject) throws IOException, ParseException {
    return Boolean.parseBoolean(readString(nextInObject, false, DEFAULT_JUMP, DEFAULT_JUMP));
  }

  public OJSONReader readNext(final char[] iUntil) throws IOException, ParseException {
    readNext(iUntil, false);
    return this;
  }

  public OJSONReader readNext(final char[] iUntil, final boolean iInclude) throws IOException, ParseException {
    readNext(iUntil, iInclude, DEFAULT_JUMP, null);
    return this;
  }
  public OJSONReader readNext(final char[] iUntil, final boolean iInclude, final char[] iJumpChars, final char[] iSkipChars)
      throws IOException, ParseException {
    readNext(iUntil, iInclude, iJumpChars, iSkipChars, true);
    return this;
  }

  public OJSONReader readNext(final char[] iUntil, final boolean iInclude, final char[] iJumpChars, final char[] iSkipChars, boolean preserveQuotes)
      throws IOException, ParseException {
    if (!in.ready())
      return this;

    jump(iJumpChars);

    if (!in.ready())
      return this;

    // READ WHILE THERE IS SOMETHING OF AVAILABLE
    int openBrackets = 0;
    char beginStringChar = ' ';
    boolean encodeMode = false;
    boolean found;
    do {
      found = false;
      if (beginStringChar == ' ') {
        // NO INSIDE A STRING
        if (openBrackets == 0) {
          // FIND FOR SEPARATOR
          for (char u : iUntil) {
            if (u == c) {
              found = true;
              break;
            }
          }
        }

        if (c == '\'' || c == '"' && !encodeMode)
          // BEGIN OF STRING
          beginStringChar = c;
        else if (c == '{')
          // OPEN EMBEDDED
          openBrackets++;
        else if (c == '}' && openBrackets > 0)
          // CLOSE EMBEDDED
          openBrackets--;

        if (!found && openBrackets == 0) {
          // FIND FOR SEPARATOR
          for (char u : iUntil) {
            if (u == c) {
              found = true;
              break;
            }
          }
        }
      } else if (beginStringChar == c && !encodeMode)
        // END OF STRING
        beginStringChar = ' ';

      if (c == '\\' && !encodeMode)
        encodeMode = true;
      else
        encodeMode = false;

      if (!found) {
        final int read = nextChar();
        if (read == -1)
          break;

        // APPEND IT
        c = (char) read;

        boolean skip = false;
        if (iSkipChars != null)
          for (char j : iSkipChars) {
            if (j == c) {
              skip = true;
              break;
            }
          }

        if (!skip && (preserveQuotes || !encodeMode)) {
          lastCharacter = c;
          buffer.append(c);
        }
      }

    } while (!found && in.ready());

    if (buffer.length() == 0)
      throw new ParseException("Expected characters '" + Arrays.toString(iUntil) + "' not found", cursor);

    if (!iInclude)
      buffer.setLength(buffer.length() - 1);

    value = buffer.toString();
    return this;
  }

  public int jump(final char[] iJumpChars) throws IOException, ParseException {
    buffer.setLength(0);

    if (!in.ready())
      return 0;

    // READ WHILE THERE IS SOMETHING OF AVAILABLE
    boolean go = true;
    while (go && in.ready()) {
      int read = nextChar();
      if (read == -1)
        return -1;

      go = false;
      for (char j : iJumpChars) {
        if (j == c) {
          go = true;
          break;
        }
      }
    }

    if (!go) {
      lastCharacter = c;
      buffer.append(c);
    }

    return c;
  }

  /**
   * Returns the next character from the input stream. Handles Unicode decoding.
   */
  public int nextChar() throws IOException {
    if (missedChar != null) {
      // RETURNS THE PREVIOUS PARSED CHAR
      c = missedChar.charValue();
      missedChar = null;

    } else {
      int read = in.read();
      if (read == -1)
        return -1;

      c = (char) read;

      if (c == '\\') {
        read = in.read();
        if (read == -1)
          return -1;

        char c2 = (char) read;
        if (c2 == 'u') {
          // DECODE UNICODE CHAR
          final StringBuilder buff = new StringBuilder(8);
          for (int i = 0; i < 4; ++i) {
            read = in.read();
            if (read == -1)
              return -1;

            buff.append((char) read);
          }

          cursor += 6;

          return (char) Integer.parseInt(buff.toString(), 16);
        } else {
          // REMEMBER THE CURRENT CHAR TO RETURN NEXT TIME
          missedChar = c2;
        }
      }
    }

    cursor++;

    if (c == NEW_LINE) {
      ++lineNumber;
      columnNumber = 0;
    } else
      ++columnNumber;

    return (char) c;
  }

  public char lastChar() {
    return lastCharacter;
  }

  public String getValue() {
    return value;
  }

  public int getLineNumber() {
    return lineNumber;
  }

  public int getColumnNumber() {
    return columnNumber;
  }

  public boolean hasNext() throws IOException {
    return in.ready();
  }
}
