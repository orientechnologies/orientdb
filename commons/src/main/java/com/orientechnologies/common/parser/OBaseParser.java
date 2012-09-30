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
package com.orientechnologies.common.parser;

import java.util.Arrays;

/**
 * Abstract generic command to parse.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OBaseParser {
  public String                   parserText;
  public String                   parserTextUpperCase;

  private transient StringBuilder parserLastWord      = new StringBuilder();
  private transient int           parserCurrentPos    = 0;
  private transient int           parserPreviousPos   = 0;
  private transient char          parserLastSeparator = ' ';

  /**
   * Throws a syntax error exception.
   * 
   * @param iText
   *          Text about the problem.
   */
  protected abstract void throwSyntaxErrorException(final String iText);

  public String getSyntax() {
    return "?";
  }

  /**
   * Parses the next word. It returns the word parsed if any.
   * 
   * @param iUpperCase
   *          True if must return UPPERCASE, otherwise false
   * @return The word parsed if any, otherwise null
   */
  protected String parserOptionalWord(final boolean iUpperCase) {
    parserPreviousPos = parserCurrentPos;

    parserNextWord(iUpperCase);
    if (parserLastWord.length() == 0)
      return null;
    return parserLastWord.toString();
  }

  /**
   * Parses the next word. If any word is parsed it's checked against the word array received as parameter. If the parsed word is
   * not enlisted in it a SyntaxError exception is thrown. It returns the word parsed if any.
   * 
   * @param iUpperCase
   *          True if must return UPPERCASE, otherwise false
   * @return The word parsed if any, otherwise null
   */
  protected String parseOptionalWord(final boolean iUpperCase, final String... iWords) {
    parserNextWord(iUpperCase);

    if (iWords.length > 0) {
      if (parserLastWord.length() == 0)
        return null;

      boolean found = false;
      for (String w : iWords) {
        if (parserLastWord.toString().equals(w)) {
          found = true;
          break;
        }
      }

      if (!found)
        throwSyntaxErrorException("Found unexpected keyword '" + parserLastWord + "' while it was expected '"
            + Arrays.toString(iWords) + "'");
    }
    return parserLastWord.toString();
  }

  /**
   * Goes back to the previous position.
   * 
   * @return The previous position
   */
  protected int parserGoBack() {
    parserCurrentPos = parserPreviousPos;
    return parserCurrentPos;
  }

  /**
   * Parses the next word. If no word is found an SyntaxError exception is thrown It returns the word parsed if any.
   * 
   * @param iUpperCase
   *          True if must return UPPERCASE, otherwise false
   * @return The word parsed
   */
  protected String parserRequiredWord(final boolean iUpperCase) {
    return parserRequiredWord(iUpperCase, "Syntax error", null);
  }

  /**
   * Parses the next word. If no word is found an SyntaxError exception with the custom message received as parameter is thrown It
   * returns the word parsed if any.
   * 
   * @param iUpperCase
   *          True if must return UPPERCASE, otherwise false
   * @param iCustomMessage
   *          Custom message to include in case of SyntaxError exception
   * @return The word parsed
   */
  protected String parserRequiredWord(final boolean iUpperCase, final String iCustomMessage) {
    return parserRequiredWord(iUpperCase, iCustomMessage, null);
  }

  /**
   * Parses the next word. If no word is found or the parsed word is not present in the word array received as parameter then a
   * SyntaxError exception with the custom message received as parameter is thrown. It returns the word parsed if any.
   * 
   * @param iUpperCase
   *          True if must return UPPERCASE, otherwise false
   * @param iCustomMessage
   *          Custom message to include in case of SyntaxError exception
   * @param iSeparators
   *          Separator characters
   * @return The word parsed
   */
  protected String parserRequiredWord(final boolean iUpperCase, final String iCustomMessage, String iSeparators) {
    if (iSeparators == null)
      iSeparators = " ()=><,\r\n";

    parserNextWord(iUpperCase, iSeparators);
    if (parserLastWord.length() == 0)
      throwSyntaxErrorException(iCustomMessage);
    return parserLastWord.toString();
  }

  /**
   * Parses the next word. If no word is found or the parsed word is not present in the word array received as parameter then a
   * SyntaxError exception is thrown.
   * 
   * @param iSeparators
   *          Array of expected keywords
   */
  protected void parserRequiredKeyword(final String... iWords) {
    parserNextWord(true, " \r\n,()");
    if (parserLastWord.length() == 0)
      throwSyntaxErrorException("Cannot find expected keyword '" + Arrays.toString(iWords) + "'");

    boolean found = false;
    for (String w : iWords) {
      if (parserLastWord.toString().equals(w)) {
        found = true;
        break;
      }
    }

    if (!found)
      throwSyntaxErrorException("Found unexpected keyword '" + parserLastWord + "' while it was expected '"
          + Arrays.toString(iWords) + "'");
  }

  /**
   * Parses the next sequence of chars.
   * 
   * @param iSeparators
   *          Array of expected keywords
   * @return The position of the word matched if any, otherwise -1 or an exception if iMandatory is true
   */
  protected int parserNextChars(final boolean iUpperCase, final boolean iMandatory, final String... iCandidateWords) {
    parserPreviousPos = parserCurrentPos;
    parserSkipWhiteSpaces();

    parserLastWord.setLength(0);

    final String[] processedWords = Arrays.copyOf(iCandidateWords, iCandidateWords.length);

    // PARSE THE CHARS
    final String text2Use = iUpperCase ? parserTextUpperCase : parserText;
    final int max = text2Use.length();

    // PARSE TILL 1 CHAR AFTER THE END TO SIMULATE A SEPARATOR AS EOF
    for (int i = 0; parserCurrentPos <= max; ++i) {
      final char ch = parserCurrentPos < max ? text2Use.charAt(parserCurrentPos) : '\n';
      final boolean separator = ch == ' ' || ch == '\r' || ch == '\n' || ch == '\t' || ch == '(';
      if (!separator)
        parserLastWord.append(ch);

      // CLEAR CANDIDATES
      int candidatesWordsCount = 0;
      int candidatesWordsPos = -1;
      for (int c = 0; c < processedWords.length; ++c) {
        final String w = processedWords[c];
        if (w != null) {
          final int wordSize = w.length();
          if ((separator && wordSize > i) || (!separator && (i > wordSize - 1 || w.charAt(i) != ch)))
            // DISCARD IT
            processedWords[c] = null;
          else {
            candidatesWordsCount++;
            if (candidatesWordsCount == 1)
              // REMEMBER THE POSITION
              candidatesWordsPos = c;
          }
        }
      }

      if (candidatesWordsCount == 1) {
        // ONE RESULT, CHECKING IF FOUND
        final String w = processedWords[candidatesWordsPos];
        if (w.length() == i + (separator ? 0 : 1) && !Character.isLetter(ch))
          // FOUND!
          return candidatesWordsPos;
      }

      if (candidatesWordsCount == 0 || separator)
        break;

      parserCurrentPos++;
    }

    if (iMandatory)
      throwSyntaxErrorException("Found unexpected keyword '" + parserLastWord + "' while it was expected '"
          + Arrays.toString(iCandidateWords) + "'");

    return -1;
  }

  /**
   * Parses optional keywords between the iWords. If a keyword is found but doesn't match with iWords then a SyntaxError is raised.
   * 
   * @param iWords
   *          Optional words to match as keyword. If at least one is passed, then the check is made
   * @return true if a keyword was found, otherwise false
   */
  protected boolean parserOptionalKeyword(final String... iWords) {
    parserNextWord(true, " \r\n,");
    if (parserLastWord.length() == 0)
      return false;

    // FOUND: CHECK IF IT'S IN RANGE
    boolean found = iWords.length == 0;
    for (String w : iWords) {
      if (parserLastWord.toString().equals(w)) {
        found = true;
        break;
      }
    }

    if (!found)
      throwSyntaxErrorException("Found unexpected keyword '" + parserLastWord + "' while it was expected '"
          + Arrays.toString(iWords) + "'");

    return true;
  }

  /**
   * Skips not valid characters like spaces and line feeds.
   * 
   * @return True if the string is not ended, otherwise false
   */
  protected boolean parserSkipWhiteSpaces() {
    if (parserCurrentPos == -1)
      return false;

    parserCurrentPos = OStringParser.jumpWhiteSpaces(parserText, parserCurrentPos, -1);
    return parserCurrentPos > -1;
  }

  /**
   * Overwrites the current cursor position.
   * 
   * @param iPosition
   *          New position
   * @return True if the string is not ended, otherwise false
   */
  protected boolean parserSetCurrentPosition(final int iPosition) {
    parserCurrentPos = iPosition;
    if (parserCurrentPos >= parserText.length())
      // END OF TEXT
      parserCurrentPos = -1;
    return parserCurrentPos > -1;
  }

  /**
   * Moves the current cursor position forward or backward of iOffset characters
   * 
   * @param iOffset
   *          Number of characters to move. Negative numbers means backwards
   * @return True if the string is not ended, otherwise false
   */
  protected boolean parserMoveCurrentPosition(final int iOffset) {
    if (parserCurrentPos < 0)
      return false;
    return parserSetCurrentPosition(parserCurrentPos + iOffset);
  }

  /**
   * Parses the next word.
   * 
   * @param iUpperCase
   *          True if must return UPPERCASE, otherwise false
   */
  protected void parserNextWord(final boolean iForceUpperCase) {
    parserNextWord(iForceUpperCase, " =><(),\r\n");
  }

  /**
   * Parses the next word.
   * 
   * @param iUpperCase
   *          True if must return UPPERCASE, otherwise false
   * @param iSeparators
   *          Separator characters
   */
  protected void parserNextWord(final boolean iForceUpperCase, final String iSeparatorChars) {
    parserPreviousPos = parserCurrentPos;
    parserLastWord.setLength(0);

    parserSkipWhiteSpaces();
    if (parserCurrentPos == -1)
      return;

    char stringBeginChar = ' ';

    final String text2Use = iForceUpperCase ? parserTextUpperCase : parserText;

    while (parserCurrentPos < text2Use.length()) {
      final char c = text2Use.charAt(parserCurrentPos);
      boolean found = false;
      for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
        if (iSeparatorChars.charAt(sepIndex) == c) {
          // SEPARATOR AT THE BEGINNING: JUMP IT
          found = true;
          break;
        }
      }
      if (!found)
        break;

      parserCurrentPos++;
    }

    try {
      int openParenthesis = 0;
      int openBraket = 0;
      int openGraph = 0;
      boolean escape = false;

      for (; parserCurrentPos < text2Use.length(); parserCurrentPos++) {
        final char c = text2Use.charAt(parserCurrentPos);

        if (!escape && c == '\\' && ((parserCurrentPos + 1) < text2Use.length())) {
          // ESCAPE CHARS
          final char nextChar = text2Use.charAt(parserCurrentPos + 1);

          if (nextChar == 'u') {
            parserCurrentPos = OStringParser.readUnicode(text2Use, parserCurrentPos + 2, parserLastWord);
          } else if (nextChar == 'n') {
            parserLastWord.append("\n");
            parserCurrentPos++;
          } else if (nextChar == 'r') {
            parserLastWord.append("\r");
            parserCurrentPos++;
          } else if (nextChar == 't') {
            parserLastWord.append("\t");
            parserCurrentPos++;
          } else if (nextChar == 'f') {
            parserLastWord.append("\f");
            parserCurrentPos++;
          } else
            escape = true;

          continue;
        }

        if (openBraket == 0 && openGraph == 0 && openParenthesis == 0 && !escape && (c == '\'' || c == '"')) {
          if (stringBeginChar != ' ') {
            // CLOSE THE STRING?
            if (stringBeginChar == c) {
              // SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
              stringBeginChar = ' ';
            }
          } else
            // START STRING
            stringBeginChar = c;
        } else if (stringBeginChar == ' ') {
          if (openBraket == 0 && openGraph == 0 && openParenthesis == 0 && parserCheckSeparator(c, iSeparatorChars)) {
            // SEPARATOR FOUND!
            break;
          } else if (c == '(')
            openParenthesis++;
          else if (c == ')' && openParenthesis > 0)
            openParenthesis--;
          else if (c == '[')
            openBraket++;
          else if (c == ']' && openBraket > 0)
            openBraket--;
          else if (c == '{')
            openGraph++;
          else if (c == '}' && openGraph > 0)
            openGraph--;
        }

        parserLastWord.append(c);

        if (escape)
          escape = false;
      }

    } finally {
      if (parserCurrentPos >= text2Use.length()) {
        // END OF TEXT
        parserCurrentPos = -1;
        parserLastSeparator = ' ';
      }
    }
  }

  /**
   * Check for a separator
   * 
   * @param c
   * @param iSeparatorChars
   * @return
   */
  private boolean parserCheckSeparator(final char c, final String iSeparatorChars) {
    for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
      if (iSeparatorChars.charAt(sepIndex) == c) {
        parserLastSeparator = c;
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the last separator encountered, otherwise returns a blank (' ').
   */
  public char parserGetLastSeparator() {
    return parserLastSeparator;
  }

  /**
   * Overwrites the last separator. To ignore it set it to blank (' ').
   */
  public void parserSetLastSeparator(final char iSeparator) {
    parserLastSeparator = iSeparator;
  }

  /**
   * Returns the cursor position before last parsing.
   * 
   * @return Offset from the beginning
   */
  public int parserGetPreviousPosition() {
    return parserPreviousPos;
  }

  /**
   * Tells if the parsing has reached the end of the content.
   * 
   * @return True if is ended, otherwise false
   */
  public boolean parserIsEnded() {
    return parserCurrentPos == -1;
  }

  /**
   * Returns the current cursor position.
   * 
   * @return Offset from the beginning
   */
  public int parserGetCurrentPosition() {
    return parserCurrentPos;
  }

  /**
   * Returns the current character in the current cursor position
   * 
   * @return The current character in the current cursor position. If the end is reached, then a blank (' ') is returned
   */
  public char parserGetCurrentChar() {
    if (parserCurrentPos < 0)
      return ' ';
    return parserText.charAt(parserCurrentPos);
  }

  /**
   * Returns the last parsed word.
   * 
   * @return Last parsed word as String
   */
  public String parserGetLastWord() {
    return parserLastWord.toString();
  }

  public static int nextWord(final String iText, final String iTextUpperCase, int ioCurrentPosition, final StringBuilder ioWord,
      final boolean iForceUpperCase) {
    return nextWord(iText, iTextUpperCase, ioCurrentPosition, ioWord, iForceUpperCase, " =><(),");
  }

  public static int nextWord(final String iText, final String iTextUpperCase, int ioCurrentPosition, final StringBuilder ioWord,
      final boolean iForceUpperCase, final String iSeparatorChars) {
    ioWord.setLength(0);

    ioCurrentPosition = OStringParser.jumpWhiteSpaces(iText, ioCurrentPosition, -1);
    if (ioCurrentPosition < 0)
      return -1;

    getWordStatic(iForceUpperCase ? iTextUpperCase : iText, ioCurrentPosition, iSeparatorChars, ioWord);

    if (ioWord.length() > 0)
      ioCurrentPosition += ioWord.length();

    return ioCurrentPosition;
  }

  /**
   * @param iText
   *          Text where to search
   * @param iBeginIndex
   *          Begin index
   * @param iSeparatorChars
   *          Separators as a String of multiple characters
   * @param ioBuffer
   *          StringBuilder object with the word found
   */
  public static void getWordStatic(final CharSequence iText, int iBeginIndex, final String iSeparatorChars,
      final StringBuilder ioBuffer) {
    ioBuffer.setLength(0);

    char stringBeginChar = ' ';
    char c;

    for (int i = iBeginIndex; i < iText.length(); ++i) {
      c = iText.charAt(i);
      boolean found = false;
      for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
        if (iSeparatorChars.charAt(sepIndex) == c) {
          // SEPARATOR AT THE BEGINNING: JUMP IT
          found = true;
          break;
        }
      }
      if (!found)
        break;

      iBeginIndex++;
    }

    for (int i = iBeginIndex; i < iText.length(); ++i) {
      c = iText.charAt(i);

      if (c == '\'' || c == '"') {
        if (stringBeginChar != ' ') {
          // CLOSE THE STRING?
          if (stringBeginChar == c) {
            // SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
            stringBeginChar = ' ';
          }
        } else {
          // START STRING
          stringBeginChar = c;
        }
      } else if (stringBeginChar == ' ') {
        for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
          if (iSeparatorChars.charAt(sepIndex) == c && ioBuffer.length() > 0) {
            // SEPARATOR (OUTSIDE A STRING): PUSH
            return;
          }
        }
      }

      ioBuffer.append(c);
    }
  }
}
