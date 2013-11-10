package com.orientechnologies.common.console;

import java.io.*;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href='mailto:enisher@gmail.com'> Artem Orobets </a>
 */
public class ODFACommandStream implements OCommandStream {
  private Reader reader;
  private CharBuffer buffer;
  private final Set<Character> separators = new HashSet<Character>(Arrays.asList(';', '\n'));

  public ODFACommandStream(String commands) {
    reader = new StringReader(commands);
    init();
  }

  public ODFACommandStream(File file) throws FileNotFoundException {
    reader = new BufferedReader(new FileReader(file));
    init();
  }

  private void init() {
    buffer = CharBuffer.allocate(1024);
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

      StringBuilder partialResult = new StringBuilder();
      State state = State.S;
      int start = 0, end = -1;
      int position = 0;
      Symbol s = null;
      while (state != State.E) {
        if (buffer.position() + position < buffer.limit()) {
          s = symbol(buffer.charAt(position));
        } else {
          buffer.compact();
          int read = reader.read(buffer);
          buffer.flip();

          if (read == 0) {
            //There is something in source, but buffer is full

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

      if (s == Symbol.EOF)
        position--;

      final String result;
      if (partialResult.length() > 0) {
        if (end > 0) {
          result = partialResult.append(buffer.subSequence(start, end + 1).toString()).toString();
        } else {
          partialResult.setLength(partialResult.length() + end + 1);
          result = partialResult.toString();
        }
      } else {
        result = buffer.subSequence(start, end + 1).toString();
      }

      buffer.position(buffer.position() + position);
      return result;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
