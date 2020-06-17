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
package com.orientechnologies.common.parser;

import java.util.ArrayList;

/**
 * String parser utility class
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OStringParser {

  public static final String WHITE_SPACE = " ";
  public static final String COMMON_JUMP = " \r\n";

  public static String[] getWords(String iRecord, final String iSeparatorChars) {
    return getWords(iRecord, iSeparatorChars, false);
  }

  public static String[] getWords(
      String iRecord, final String iSeparatorChars, final boolean iIncludeStringSep) {
    return getWords(iRecord, iSeparatorChars, " \n\r\t", iIncludeStringSep);
  }

  public static String[] getWords(
      String iText,
      final String iSeparatorChars,
      final String iJumpChars,
      final boolean iIncludeStringSep) {
    iText = iText.trim();

    final ArrayList<String> fields = new ArrayList<String>();
    final StringBuilder buffer = new StringBuilder(64);
    char stringBeginChar = ' ';
    char c;
    int openBraket = 0;
    int openGraph = 0;
    boolean charFound;
    boolean escape = false;

    for (int i = 0; i < iText.length(); ++i) {
      c = iText.charAt(i);

      if (!escape && c == '\\' && ((i + 1) < iText.length())) {
        // ESCAPE CHARS
        final char nextChar = iText.charAt(i + 1);

        if (nextChar == 'u') {
          i = readUnicode(iText, i + 2, buffer);
        } else if (nextChar == 'n') {
          buffer.append(stringBeginChar == ' ' ? "\n" : "\\\n");
          i++;
        } else if (nextChar == 'r') {
          buffer.append(stringBeginChar == ' ' ? "\r" : "\\\r");
          i++;
        } else if (nextChar == 't') {
          buffer.append(stringBeginChar == ' ' ? "\t" : "\\\t");
          i++;
        } else if (nextChar == 'f') {
          buffer.append(stringBeginChar == ' ' ? "\f" : "\\\f");
          i++;
        } else if (stringBeginChar != ' ' && nextChar == '\'' || nextChar == '"') {
          buffer.append('\\');
          buffer.append(nextChar);
          i++;
        } else {
          buffer.append('\\');
          escape = true;
        }

        continue;
      }

      if (!escape && (c == '\'' || c == '"')) {
        if (stringBeginChar != ' ') {
          // CLOSE THE STRING?
          if (stringBeginChar == c) {
            // SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
            stringBeginChar = ' ';

            if (iIncludeStringSep) buffer.append(c);
            continue;
          }
        } else {
          // START STRING
          stringBeginChar = c;
          if (iIncludeStringSep) buffer.append(c);

          continue;
        }
      } else if (stringBeginChar == ' ') {
        if (c == '[') openBraket++;
        else if (c == ']') openBraket--;
        else if (c == '{') openGraph++;
        else if (c == '}') openGraph--;
        else if (openBraket == 0 && openGraph == 0) {
          charFound = false;
          for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
            if (iSeparatorChars.charAt(sepIndex) == c) {
              charFound = true;
              if (buffer.length() > 0) {
                // SEPARATOR (OUTSIDE A STRING): PUSH
                fields.add(buffer.toString());
                buffer.setLength(0);
              }
              break;
            }
          }

          if (charFound) continue;
        }

        if (stringBeginChar == ' ') {
          // CHECK FOR CHAR TO JUMP
          charFound = false;

          for (int jumpIndex = 0; jumpIndex < iJumpChars.length(); ++jumpIndex) {
            if (iJumpChars.charAt(jumpIndex) == c) {
              charFound = true;
              break;
            }
          }

          if (charFound) continue;
        }
      }

      buffer.append(c);

      if (escape) escape = false;
    }

    if (buffer.length() > 0) {
      // ADD THE LAST WORD IF ANY
      fields.add(buffer.toString());
    }

    String[] result = new String[fields.size()];
    fields.toArray(result);
    return result;
  }

  public static String[] split(String iText, final char iSplitChar, String iJumpChars) {
    iText = iText.trim();

    ArrayList<String> fields = new ArrayList<String>();
    StringBuilder buffer = new StringBuilder(256);
    char c;
    char stringChar = ' ';
    boolean escape = false;
    boolean jumpSplitChar = false;
    boolean charFound;

    for (int i = 0; i < iText.length(); i++) {
      c = iText.charAt(i);

      if (!escape && c == '\\' && ((i + 1) < iText.length())) {
        if (iText.charAt(i + 1) == 'u') {
          i = readUnicode(iText, i + 2, buffer);
        } else {
          escape = true;
          buffer.append(c);
        }
        continue;
      }

      if (c == '\'' || c == '"') {
        if (!jumpSplitChar) {
          jumpSplitChar = true;
          stringChar = c;
        } else {
          if (!escape && c == stringChar) jumpSplitChar = false;
        }
      }

      if (c == iSplitChar) {
        if (!jumpSplitChar) {
          fields.add(buffer.toString());
          buffer.setLength(0);
          continue;
        }
      }

      // CHECK IF IT MUST JUMP THE CHAR
      if (buffer.length() == 0) {
        charFound = false;

        for (int jumpIndex = 0; jumpIndex < iJumpChars.length(); ++jumpIndex) {
          if (iJumpChars.charAt(jumpIndex) == c) {
            charFound = true;
            break;
          }
        }

        if (charFound) continue;
      }

      buffer.append(c);

      if (escape) escape = false;
    }

    if (buffer.length() > 0) {
      fields.add(buffer.toString());
      buffer.setLength(0);
    }
    String[] result = new String[fields.size()];
    fields.toArray(result);
    return result;
  }

  /**
   * Finds a character inside a string specyfing the limits and direction. If iFrom is minor than
   * iTo, then it moves forward, otherwise backward.
   */
  public static int indexOfOutsideStrings(
      final String iText, final char iToFind, int iFrom, int iTo) {
    if (iTo == -1) iTo = iText.length() - 1;
    if (iFrom == -1) iFrom = iText.length() - 1;

    char c;
    char stringChar = ' ';
    boolean escape = false;

    final StringBuilder buffer = new StringBuilder(1024);

    int i = iFrom;
    while (true) {
      c = iText.charAt(i);

      if (!escape && c == '\\' && ((i + 1) < iText.length())) {
        if (iText.charAt(i + 1) == 'u') {
          i = readUnicode(iText, i + 2, buffer);
        } else escape = true;
      } else {
        if (c == '\'' || c == '"') {
          // BEGIN/END STRING
          if (stringChar == ' ') {
            // BEGIN
            stringChar = c;
          } else {
            // END
            if (!escape && c == stringChar) stringChar = ' ';
          }
        }

        if (c == iToFind && stringChar == ' ') return i;

        if (escape) escape = false;
      }

      if (iFrom < iTo) {
        // MOVE FORWARD
        if (++i > iTo) break;
      } else {
        // MOVE BACKWARD
        if (--i < iFrom) break;
      }
    }
    return -1;
  }

  /**
   * Jump white spaces.
   *
   * @param iText String to analyze
   * @param iCurrentPosition Current position in text
   * @param iMaxPosition TODO
   * @return The new offset inside the string analyzed
   */
  public static int jumpWhiteSpaces(
      final CharSequence iText, final int iCurrentPosition, final int iMaxPosition) {
    return jump(iText, iCurrentPosition, iMaxPosition, COMMON_JUMP);
  }

  /**
   * Jump some characters reading from an offset of a String.
   *
   * @param iText String to analyze
   * @param iCurrentPosition Current position in text
   * @param iMaxPosition Maximum position to read
   * @param iJumpChars String as char array of chars to jump
   * @return The new offset inside the string analyzed
   */
  public static int jump(
      final CharSequence iText,
      int iCurrentPosition,
      final int iMaxPosition,
      final String iJumpChars) {
    if (iCurrentPosition < 0) return -1;

    final int size = iMaxPosition > -1 ? Math.min(iMaxPosition, iText.length()) : iText.length();
    final int jumpCharSize = iJumpChars.length();
    boolean found = true;
    char c;
    for (; iCurrentPosition < size; ++iCurrentPosition) {
      found = false;
      c = iText.charAt(iCurrentPosition);
      for (int jumpIndex = 0; jumpIndex < jumpCharSize; ++jumpIndex) {
        if (iJumpChars.charAt(jumpIndex) == c) {
          found = true;
          break;
        }
      }

      if (!found) break;
    }

    return iCurrentPosition >= size ? -1 : iCurrentPosition;
  }

  public static int readUnicode(String iText, int position, final StringBuilder buffer) {
    // DECODE UNICODE CHAR
    final StringBuilder buff = new StringBuilder(64);
    final int lastPos = position + 4;
    for (; position < lastPos; ++position) buff.append(iText.charAt(position));

    buffer.append((char) Integer.parseInt(buff.toString(), 16));
    return position - 1;
  }

  public static int readUnicode(char[] iText, int position, final StringBuilder buffer) {
    // DECODE UNICODE CHAR
    final StringBuilder buff = new StringBuilder(64);
    final int lastPos = position + 4;
    for (; position < lastPos; ++position) buff.append(iText[position]);

    buffer.append((char) Integer.parseInt(buff.toString(), 16));
    return position - 1;
  }

  public static String replaceAll(
      final String iText, final String iToReplace, final String iReplacement) {
    if (iText == null || iText.length() <= 0 || iToReplace == null || iToReplace.length() <= 0)
      return iText;
    int pos = iText.indexOf(iToReplace);
    int lastAppend = 0;
    final StringBuffer buffer = new StringBuffer(1024);
    while (pos > -1) {
      buffer.append(iText.substring(lastAppend, pos));
      buffer.append(iReplacement);
      lastAppend = pos + iToReplace.length();
      pos = iText.indexOf(iToReplace, lastAppend);
    }
    buffer.append(iText.substring(lastAppend));
    return buffer.toString();
  }

  /** Like String.startsWith() but ignoring case */
  public static boolean startsWithIgnoreCase(final String iText, final String iToFind) {
    if (iText.length() < iToFind.length()) return false;

    return iText.substring(0, iToFind.length()).equalsIgnoreCase(iToFind);
  }
}
