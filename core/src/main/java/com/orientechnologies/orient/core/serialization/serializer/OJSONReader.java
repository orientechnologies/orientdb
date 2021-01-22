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
package com.orientechnologies.orient.core.serialization.serializer;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.executor.ORidSet;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OJSONReader {
  public static final char NEW_LINE = '\n';
  public static final char[] DEFAULT_JUMP = new char[] {' ', '\r', '\n', '\t'};
  // public static final char[] DEFAULT_SKIP = new char[] {'\r', '\n', '\t'};
  public static final char[] BEGIN_OBJECT = new char[] {'{'};
  public static final char[] END_OBJECT = new char[] {'}'};
  public static final char[] FIELD_ASSIGNMENT = new char[] {':'};
  // public static final char[] BEGIN_STRING = new char[] {'"'};
  public static final char[] COMMA_SEPARATOR = new char[] {','};
  public static final char[] NEXT_IN_OBJECT = new char[] {',', '}'};
  public static final char[] NEXT_IN_ARRAY = new char[] {',', ']'};
  public static final char[] NEXT_OBJ_IN_ARRAY = new char[] {'{', ']'};
  public static final char[] ANY_NUMBER =
      new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
  public static final char[] BEGIN_COLLECTION = new char[] {'['};
  public static final char[] END_COLLECTION = new char[] {']'};

  private BufferedReader in;
  private int cursor = 0;
  private int lineNumber = 0;
  private int columnNumber = 0;
  private StringBuilder buffer = new StringBuilder(16384); // 16KB
  private String value;
  private char c;
  private char lastCharacter;
  private Character missedChar;

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

  public int readNumber(final char[] iUntil, final boolean iInclude)
      throws IOException, ParseException {
    if (readNext(iUntil, iInclude) == null) throw new ParseException("Expected integer", cursor);

    return Integer.parseInt(value.trim());
  }

  public String readString(final char[] iUntil) throws IOException, ParseException {
    return readString(iUntil, false);
  }

  public String readString(final char[] iUntil, final boolean iInclude)
      throws IOException, ParseException {
    return readString(iUntil, iInclude, DEFAULT_JUMP, null);
  }

  public String readString(
      final char[] iUntil, final boolean iInclude, final char[] iJumpChars, final char[] iSkipChars)
      throws IOException, ParseException {
    if (readNext(iUntil, iInclude, iJumpChars, iSkipChars) == null) return null;

    if (!iInclude && value.startsWith("\"")) {
      return value.substring(1, value.lastIndexOf("\""));
    }

    return value;
  }

  public String readString(
      final char[] iUntil,
      final boolean iInclude,
      final char[] iJumpChars,
      final char[] iSkipChars,
      boolean preserveQuotes)
      throws IOException, ParseException {
    if (readNext(iUntil, iInclude, iJumpChars, iSkipChars, preserveQuotes) == null) return null;

    if (!iInclude && value.startsWith("\"")) {
      return value.substring(1, value.lastIndexOf("\""));
    }

    return value;
  }

  /**
   * @param maxRidbagSizeLazyImport
   * @return a pair containing as a key the parsed record string (with big ridbags emptied), and as
   *     a value the map of big ridbag field names and content
   * @throws IOException
   * @throws ParseException
   */
  public OPair<String, Map<String, ORidSet>> readRecordString(int maxRidbagSizeLazyImport)
      throws IOException, ParseException {
    Map<String, ORidSet> ridbags =
        readNextRecord(
            OJSONReader.NEXT_IN_ARRAY,
            false,
            OJSONReader.DEFAULT_JUMP,
            null,
            true,
            maxRidbagSizeLazyImport);
    if (ridbags == null) return null;

    String resultValue = value;
    if (value.startsWith("\"")) {
      resultValue = value.substring(1, value.lastIndexOf("\""));
    }

    return new OPair(resultValue, ridbags);
  }

  public boolean readBoolean(final char[] nextInObject) throws IOException, ParseException {
    return Boolean.parseBoolean(readString(nextInObject, false, DEFAULT_JUMP, DEFAULT_JUMP));
  }

  public OJSONReader readNext(final char[] iUntil) throws IOException, ParseException {
    readNext(iUntil, false);
    return this;
  }

  public OJSONReader readNext(final char[] iUntil, final boolean iInclude)
      throws IOException, ParseException {
    readNext(iUntil, iInclude, DEFAULT_JUMP, null);
    return this;
  }

  public OJSONReader readNext(
      final char[] iUntil, final boolean iInclude, final char[] iJumpChars, final char[] iSkipChars)
      throws IOException, ParseException {
    readNext(iUntil, iInclude, iJumpChars, iSkipChars, true);
    return this;
  }

  public OJSONReader readNext(
      final char[] iUntil,
      final boolean iInclude,
      final char[] iJumpChars,
      final char[] iSkipChars,
      boolean preserveQuotes)
      throws IOException, ParseException {
    if (!in.ready()) return this;

    jump(iJumpChars);

    if (!in.ready()) return this;

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

      if (c == '\\' && !encodeMode) encodeMode = true;
      else encodeMode = false;

      if (!found) {
        final int read = nextChar();
        if (read == -1) break;

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
      throw new ParseException(
          "Expected characters '" + Arrays.toString(iUntil) + "' not found", cursor);

    if (!iInclude) buffer.setLength(buffer.length() - 1);

    value = buffer.toString();
    return this;
  }

  public Map<String, ORidSet> readNextRecord(
      final char[] iUntil,
      final boolean iInclude,
      final char[] iJumpChars,
      final char[] iSkipChars,
      boolean preserveQuotes,
      int maxRidbagSizeLazyImport)
      throws IOException, ParseException {
    if (!in.ready()) return Collections.emptyMap();

    jump(iJumpChars);

    if (!in.ready()) return Collections.emptyMap();

    Map<String, ORidSet> result = new HashMap<>();

    Pattern ridPattern = Pattern.compile("\"#([0-9]+):([0-9]+)\"");
    ORidSet ridbagSet = null;
    StringBuilder lastString = null;
    String lastFieldName = null;
    StringBuilder lastCollection = null;

    // READ WHILE THERE IS SOMETHING OF AVAILABLE
    int openBrackets = 0;
    int openSquare = 0;
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

        if (c == '\'' || c == '"' && !encodeMode) {
          // BEGIN OF STRING
          beginStringChar = c;
          lastString = new StringBuilder();
        } else if (c == '{')
          // OPEN EMBEDDED
          openBrackets++;
        else if (c == '}' && openBrackets > 0)
          // CLOSE EMBEDDED
          openBrackets--;
        else if (c == '[') {
          if (openSquare == 0
              && (lastString.toString().startsWith("out_")
                  || lastString.toString().startsWith("in_"))) {
            lastCollection = new StringBuilder();
            lastFieldName = lastString.toString();
            lastFieldName = lastFieldName.substring(0, lastFieldName.length() - 1);
            ridbagSet = new ORidSet();
          }
          openSquare++;
        } else if (c == ']' && openSquare == 1) {
          if (lastFieldName != null && ridbagSet != null && ridbagSet.size() > 0) {
            boolean ridbagAdderd = stringToRidbag(lastCollection, ridbagSet, ridPattern);
            result.put(lastFieldName, ridbagSet);
            lastFieldName = null;
            ridbagSet = null;
            if (ridbagAdderd) {
              buffer.append("]");
            }
          }
          openSquare--;
        }

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

      if (c == '\\' && !encodeMode) encodeMode = true;
      else encodeMode = false;

      if (!found) {
        final int read = nextChar();
        if (read == -1) break;

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
          if (openSquare == 0 && lastCollection != null && lastFieldName != null) {
            buffer.append(lastCollection);
            buffer.append(c);
            lastCollection = null;
          } else if (openSquare > 0 && lastCollection != null) {
            lastCollection.append(c);
            if (lastCollection.length() > maxRidbagSizeLazyImport
                && lastFieldName != null
                && lastCollection != null) {
              // preprocess RIDs
              if (!stringToRidbag(lastCollection, ridbagSet, ridPattern)) {
                lastFieldName = null;
                //                openBrackets = 0;
              }
            }
          } else {
            buffer.append(c);
          }
          if (beginStringChar != ' ') {
            lastString.append(c);
          }
        }
      }

    } while (!found && in.ready());

    if (buffer.length() == 0)
      throw new ParseException(
          "Expected characters '" + Arrays.toString(iUntil) + "' not found", cursor);

    if (!iInclude) buffer.setLength(buffer.length() - 1);

    value = buffer.toString();
    return result;
  }

  private boolean stringToRidbag(
      StringBuilder lastCollection, ORidSet ridbagSet, Pattern ridPattern) {
    String collectionString = lastCollection.toString();

    if (collectionString.startsWith(",") && collectionString.endsWith("]")) {
      collectionString = collectionString.substring(1, collectionString.length() - 1);
    } else if (collectionString.endsWith("]")) {
      collectionString = collectionString.substring(0, collectionString.length() - 1);
    } else if (collectionString.startsWith(",")) {
      collectionString = collectionString.substring(1);
    }
    String[] split = collectionString.split(",");

    int i = 0;
    while (i < split.length) {
      Matcher matcher = ridPattern.matcher(split[i]);
      boolean matches = matcher.matches();
      if (i == 0 && !matches) {
        buffer.append(lastCollection);
        return false;
      }
      if (!matches) {
        break;
      }
      ORID rid = new ORecordId(split[i].substring(1, split[i].length() - 1));
      ridbagSet.add(rid);
      i++;
    }
    lastCollection.setLength(0);
    for (int j = i; j < split.length; j++) {
      if (j != i || lastCollection.toString().startsWith(",")) {
        lastCollection.append(",");
      }
      lastCollection.append(split[j]);
    }
    if (collectionString.endsWith(",") && !lastCollection.toString().endsWith(",")) {
      lastCollection.append(",");
    }
    return true;
  }

  public int jump(final char[] iJumpChars) throws IOException, ParseException {
    buffer.setLength(0);

    if (!in.ready()) return 0;

    // READ WHILE THERE IS SOMETHING OF AVAILABLE
    boolean go = true;
    while (go && in.ready()) {
      int read = nextChar();
      if (read == -1) return -1;

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

  /** Returns the next character from the input stream. Handles Unicode decoding. */
  public int nextChar() throws IOException {
    if (missedChar != null) {
      // RETURNS THE PREVIOUS PARSED CHAR
      c = missedChar.charValue();
      missedChar = null;

    } else {
      int read = in.read();
      if (read == -1) return -1;

      c = (char) read;

      if (c == '\\') {
        read = in.read();
        if (read == -1) return -1;

        char c2 = (char) read;
        if (c2 == 'u') {
          // DECODE UNICODE CHAR
          final StringBuilder buff = new StringBuilder(8);
          for (int i = 0; i < 4; ++i) {
            read = in.read();
            if (read == -1) return -1;

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
    } else ++columnNumber;

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
