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

package com.orientechnologies.common.console;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/** @author Luigi Dell'Aquila */
public class OConsoleCommandStream implements OCommandStream {
  public static final int BUFFER_SIZE = 1024;
  private Reader reader;

  private Character nextCharacter;
  private State state;
  private int nestingLevel = 0;

  private enum State {
    TEXT,
    SINGLE_QUOTE_STRING,
    DOUBLE_QUOTE_STRING,
    SINGLE_LINE_COMMENT,
    MULTI_LINE_COMMENT,
    HYPHEN,
    SLASH,
    CLOSING_ASTERISK,
    ESCAPING_IN_SINGLE_QUOTE_STRING,
    ESCAPING_IN_DOUBLE_QUOTE_STRING
  }

  private enum Symbol {
    LETTER, // normal letters

    DOUBLE_QUOTE,
    SINGLE_QUOTE, // quotes

    HYPHEN,
    POUND, // single row comments

    SLASH,
    ASTERISK, // multi-row comments

    SEPARATOR, // command separator

    NEW_LINE, // command separator

    STRING_ESCAPE, // backslash, string escape

    LEFT_BRACKET,
    RIGHT_BRAKET, // { and }, for blocks

    EOF // end of file
  }

  public OConsoleCommandStream(String commands) {
    reader = new StringReader(commands);

    init();
  }

  public OConsoleCommandStream(File file) throws FileNotFoundException {
    reader = new BufferedReader(new FileReader(file), BUFFER_SIZE);

    init();
  }

  private void init() {
    try {
      final int next = reader.read();
      if (next > -1) nextCharacter = (char) next;
      else nextCharacter = null;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Character nextCharacter() throws IOException {
    if (nextCharacter == null) return null;

    final Character result = nextCharacter;
    final int next = reader.read();
    if (next < 0) nextCharacter = null;
    else nextCharacter = (char) next;

    return result;
  }

  @Override
  public boolean hasNext() {
    return nextCharacter != null;
  }

  @Override
  public String nextCommand() {
    try {
      state = State.TEXT;
      final StringBuilder result = new StringBuilder();

      while (true) {
        Character c = nextCharacter();
        Symbol symbol = symbol(c);

        switch (state) {
          case TEXT:
            switch (symbol) {
              case LETTER:
                result.append(c);
                break;
              case DOUBLE_QUOTE:
                result.append(c);
                state = State.DOUBLE_QUOTE_STRING;
                break;
              case SINGLE_QUOTE:
                result.append(c);
                state = State.SINGLE_QUOTE_STRING;
                break;
              case LEFT_BRACKET:
                result.append(c);
                nestingLevel++;
                break;
              case RIGHT_BRAKET:
                result.append(c);
                nestingLevel--;
                if (nestingLevel <= 0 && isControlBlock(result)) {
                  return result.toString().trim();
                }
                break;
              case HYPHEN:
                if (result.toString().trim().length() == 0) {
                  // allow commands only at the beginning of a row
                  state = State.HYPHEN;
                } else {
                  result.append("-");
                }
                break;
              case POUND:
                if (result.toString().trim().length() == 0) {
                  // otherwise it could just be a RID
                  state = State.SINGLE_LINE_COMMENT;
                } else {
                  result.append("#");
                }
                break;
              case SLASH:
                state = State.SLASH;
                break;
              case STRING_ESCAPE:
              case ASTERISK:
                result.append(c);
                break;
              case SEPARATOR:
              case NEW_LINE:
                if (nestingLevel <= 0) {
                  state = State.TEXT;
                  return result.toString().trim();
                } else {
                  result.append("\n");
                }
                break;
              case EOF:
                state = State.TEXT;
                return result.toString().trim();
            }
            break;

          case SINGLE_QUOTE_STRING:
            if (symbol == Symbol.EOF) {
              return result.toString().trim();
            }
            if (symbol == Symbol.STRING_ESCAPE) {
              state = State.ESCAPING_IN_SINGLE_QUOTE_STRING;
              break;
            }
            if (symbol == Symbol.SINGLE_QUOTE) {
              state = State.TEXT;
            }
            result.append(c);
            break;
          case DOUBLE_QUOTE_STRING:
            if (symbol == Symbol.EOF) {
              return result.toString().trim();
            }
            if (symbol == Symbol.STRING_ESCAPE) {
              state = State.ESCAPING_IN_DOUBLE_QUOTE_STRING;
              break;
            }
            if (symbol == Symbol.DOUBLE_QUOTE) {
              state = State.TEXT;
            }
            result.append(c);
            break;
          case SINGLE_LINE_COMMENT:
            if (symbol == Symbol.NEW_LINE || symbol == Symbol.EOF) {
              state = State.TEXT;
              return result.toString();
            }
            break;
          case MULTI_LINE_COMMENT:
            if (symbol == Symbol.EOF) {
              return result.toString();
            }
            if (symbol == Symbol.ASTERISK) {
              state = State.CLOSING_ASTERISK;
            }
            break;
          case HYPHEN: // found a hyphen, if there is another one, it's a comment
            if (symbol == Symbol.EOF) {
              state = State.TEXT;
              result.append("-");
              return result.toString().trim();
            }
            if (symbol == Symbol.HYPHEN) {
              state = State.SINGLE_LINE_COMMENT;
            } else {
              result.append("-");
              result.append(c);
              state = State.TEXT;
            }
            break;
          case SLASH: // found a slash, if there is an asterisk it is a multi-line comment
            if (symbol == Symbol.EOF) {
              result.append("/");
              return result.toString().trim();
            }
            if (symbol == Symbol.ASTERISK) {
              state = State.MULTI_LINE_COMMENT;
            } else {
              state = State.TEXT;
              result.append("/");
              result.append(c);
            }
            break;
          case CLOSING_ASTERISK: // you are in a multi-line comment and found an asterisk, if there
            // is a slash it's closing the comment
            if (symbol == Symbol.EOF) {
              return result.toString().trim();
            }
            if (symbol == Symbol.SLASH) {
              state = State.TEXT;
            }
            break;
          case ESCAPING_IN_SINGLE_QUOTE_STRING:
            if (symbol == Symbol.EOF) {
              result.append('\\');
              return result.toString().trim();
            }
            result.append('\\');
            result.append(c);
            state = State.SINGLE_QUOTE_STRING;
            break;
          case ESCAPING_IN_DOUBLE_QUOTE_STRING:
            if (symbol == Symbol.EOF) {
              result.append('\\');
              return result.toString().trim();
            }
            result.append('\\');
            result.append(c);
            state = State.DOUBLE_QUOTE_STRING;
            break;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isControlBlock(StringBuilder result) {
    String cmd = result.toString().trim();
    if (cmd.length() < 6) {
      return false;
    }
    if (cmd.substring(0, 6).equalsIgnoreCase("if ")) {
      return true;
    }
    if (cmd.substring(0, 6).equalsIgnoreCase("foreach ")) {
      return true;
    }
    if (cmd.substring(0, 6).equalsIgnoreCase("while ")) {
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Symbol symbol(Character c) {
    if (c == null) return Symbol.EOF;
    if (c.equals('\'')) return Symbol.SINGLE_QUOTE;
    if (c.equals('"')) return Symbol.DOUBLE_QUOTE;
    if (c.equals('-')) return Symbol.HYPHEN;
    if (c.equals('#')) return Symbol.POUND;
    if (c.equals('/')) return Symbol.SLASH;
    if (c.equals('*')) return Symbol.ASTERISK;
    if (c.equals(';')) return Symbol.SEPARATOR;
    if (c.equals('{')) return Symbol.LEFT_BRACKET;
    if (c.equals('}')) return Symbol.RIGHT_BRAKET;
    if (c.equals('\n') || c.equals('\r')) return Symbol.NEW_LINE;
    if (c.equals('\\')) {
      return Symbol.STRING_ESCAPE;
    }

    return Symbol.LETTER;
  }
}
