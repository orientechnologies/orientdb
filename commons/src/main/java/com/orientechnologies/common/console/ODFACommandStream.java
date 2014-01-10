package com.orientechnologies.common.console;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href='mailto:enisher@gmail.com'> Artem Orobets </a>
 */
public class ODFACommandStream implements OCommandStream {
  public static final int      BUFFER_SIZE = 1024;

  private Reader               reader;
  private CharBuffer           buffer;
  private final Set<Character> separators  = new HashSet<Character>(Arrays.asList(';', '\n'));
  private int                  position;
  private int                  start;
  private int                  end;
  private StringBuilder        partialResult;
  private State                state;

  public ODFACommandStream(String commands) {
    reader = new StringReader(commands);
    init();
  }

  public ODFACommandStream(File file) throws FileNotFoundException {
    reader = new BufferedReader(new FileReader(file));
    init();
  }

  private void init() {
    buffer = CharBuffer.allocate(BUFFER_SIZE);
    buffer.flip();
  }

  @Override
  public boolean hasNext() {
    try {
      fillBuffer();

      return buffer.hasRemaining();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void fillBuffer() throws IOException {
    if (!buffer.hasRemaining()) {
      buffer.clear();
      reader.read(buffer);
      buffer.flip();
    }
  }

  @Override
  public String nextCommand() {
    try {
      fillBuffer();

      partialResult = new StringBuilder();
      state = State.S;
      start = 0;
      end = -1;
      position = 0;
      Symbol s = null;
      while (state != State.E) {
        s = nextSymbol();

        final State newState = transition(state, s);

        if (state == State.S && newState != State.S)
          start = position;

        if (newState == State.A)
          end = position;

        if (newState == State.F)
          throw new IllegalStateException("Unexpected end of file");

        state = newState;
        position++;
      }

      if (s == Symbol.EOF) {
        position--;
        if (end == -1) {
          start = 0;
          end = 0;
        }
      }

      String result;
      if (partialResult.length() > 0) {
        if (end > 0) {
          result = partialResult.append(buffer.subSequence(start, end + 1).toString()).toString();
        } else {
          partialResult.setLength(partialResult.length() + end + 1);
          result = partialResult.toString();
        }
      } else {
        // DON'T PUT THIS ON ONE LINE ONLY BECAUSE WITH JDK6 subSequence() RETURNS A CHAR CharSequence while JDK7+ RETURNS
        // CharBuffer
        final CharSequence cs = buffer;
        result = cs.subSequence(start, end + 1).toString();
      }

      buffer.position(buffer.position() + position);
      return result;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Symbol nextSymbol() throws IOException {
    Symbol s;
    if (buffer.position() + position < buffer.limit()) {
      s = symbol(buffer.charAt(position));
    } else {
      buffer.compact();
      int read = reader.read(buffer);
      buffer.flip();

      if (read == 0) {
        // There is something in source, but buffer is full

        if (state != State.S)
          partialResult.append(buffer.subSequence(start, position).toString());
        start = 0;
        end = end - position;
        buffer.clear();
        read = reader.read(buffer);
        buffer.flip();
        position = 0;
      }

      if (read == -1) {
        s = Symbol.EOF;
      } else {
        s = symbol(buffer.charAt(position));
      }
    }
    return s;
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

  private enum State {
    S, A, B, C, D, E, F
  }

  private enum Symbol {
    LATTER, WS, QT, AP, SEP, EOF
  }
}
