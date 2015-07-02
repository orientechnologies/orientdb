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

package com.orientechnologies.common.console;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ODFACommandStream implements OCommandStream {
  public static final int      BUFFER_SIZE = 1024;
  private final Set<Character> separators  = new HashSet<Character>(Arrays.asList(';', '\n'));
  private Reader               reader;

  private Character            nextCharacter;
  private State                state;

  private enum State {
    S, A, B, C, D, E, F
  }

  private enum Symbol {
    LATTER, WS, QT, AP, SEP, EOF
  }

  public ODFACommandStream(String commands) {
    reader = new StringReader(commands);

    init();
  }

  public ODFACommandStream(File file) throws FileNotFoundException {
    reader = new BufferedReader(new FileReader(file), BUFFER_SIZE);

    init();
  }

  private void init() {
    try {
      final int next = reader.read();
      if (next > -1)
        nextCharacter = (char) next;
      else
        nextCharacter = null;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Character nextCharacter() throws IOException {
    if (nextCharacter == null)
      return null;

    final Character result = nextCharacter;
    final int next = reader.read();
    if (next < 0)
      nextCharacter = null;
    else
      nextCharacter = (char) next;

    return result;
  }

  @Override
  public boolean hasNext() {
    return nextCharacter != null;
  }

  @Override
  public String nextCommand() {
    try {
      state = State.S;
      final StringBuilder result = new StringBuilder();

      StringBuilder stateWord = new StringBuilder();

      while (state != State.E) {
        Character c = nextCharacter();
        String sch = null;

        Symbol s;
        if (c == null)
          s = Symbol.EOF;
        else if (c.equals('\''))
          s = Symbol.AP;
        else if (c.equals('"'))
          s = Symbol.QT;
        else if (separators.contains(c))
          s = Symbol.SEP;
        else if (Character.isWhitespace(c))
          s = Symbol.WS;
        else if (c == '\\') {
          final Character nextCharacter = nextCharacter();

          sch = "" + c + nextCharacter;
          s = Symbol.LATTER;
        } else
          s = Symbol.LATTER;

        final State newState = transition(state, s);

        if (newState == State.F)
          throw new IllegalStateException("Unexpected end of file");

        State oldState = state;
        state = newState;

        if (state != State.E && state != State.S) {
          if (state != oldState) {
            result.append(stateWord);
            stateWord = new StringBuilder();
          }

          if (sch != null)
            stateWord.append(sch);
          else
            stateWord.append(c);
        }

        if (state == State.E) {
          if (stateWord.length() > 0 && (oldState != State.D))
            result.append(stateWord);
        }
      }

      return result.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    if (c.equals('\''))
      return Symbol.AP;
    if (c.equals('"'))
      return Symbol.QT;
    if (separators.contains(c))
      return Symbol.SEP;
    if (Character.isWhitespace(c))
      return Symbol.WS;

    return Symbol.LATTER;
  }

  private State transition(State s, Symbol c) {
    switch (s) {
    case S:
      switch (c) {
      case LATTER:
        return State.A;
      case WS:
        return State.S;
      case AP:
        return State.B;
      case QT:
        return State.C;
      case SEP:
        return State.S;
      case EOF:
        return State.E;
      }
      break;
    case A:
    case D:
      switch (c) {
      case LATTER:
        return State.A;
      case WS:
        return State.D;
      case AP:
        return State.B;
      case QT:
        return State.C;
      case SEP:
        return State.E;
      case EOF:
        return State.E;
      }
      break;
    case B:
      switch (c) {
      case LATTER:
        return State.B;
      case WS:
        return State.B;
      case AP:
        return State.A;
      case QT:
        return State.B;
      case SEP:
        return State.B;
      case EOF:
        return State.F;
      }
      break;
    case C:
      switch (c) {
      case LATTER:
        return State.C;
      case WS:
        return State.C;
      case AP:
        return State.C;
      case QT:
        return State.A;
      case SEP:
        return State.C;
      case EOF:
        return State.F;
      }
      break;
    case E:
      return State.E;
    case F:
      return State.F;
    }

    throw new IllegalStateException();
  }
}
