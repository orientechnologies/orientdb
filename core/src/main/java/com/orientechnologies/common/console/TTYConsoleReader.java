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

import com.orientechnologies.common.exception.OSystemException;
import com.orientechnologies.common.log.OLogManager;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/** Custom implementation of TTY reader. Supports arrow keys + history. */
public class TTYConsoleReader implements OConsoleReader {
  public static final int HORIZONTAL_TAB_CHAR = 9;
  public static final int NEW_LINE_CHAR = 10;
  public static final int VERTICAL_TAB_CHAR = 11;
  public static final int ESC = 27;
  public static final int UNIT_SEPARATOR_CHAR = 31;
  public static final int CTRL = 53;
  public static final int SEMICOLON = 59;
  public static final int UP_CHAR = 65;
  public static final int DOWN_CHAR = 66;
  public static final int RIGHT_CHAR = 67;
  public static final int LEFT_CHAR = 68;
  public static final int END_CHAR = 70;
  public static final int BEGIN_CHAR = 72;
  public static final int BACKSLASH = 92;
  public static final int DEL_CHAR = 126;
  public static final int BACKSPACE_CHAR = 127;
  private static final int MAX_HISTORY_ENTRIES = 50;
  private static final Object signalLock = new Object();
  private static String HISTORY_FILE_NAME = ".orientdb_history";
  private static String ORIENTDB_HOME_DIR = ".orientdb";
  private static int cachedConsoleWidth = -1; // -1 for no cached value, -2 to indicate the error

  static {
    final Signal signal = new Signal("WINCH");
    Signal.handle(
        signal,
        new SignalHandler() {
          @Override
          public void handle(Signal signal) {
            synchronized (signalLock) {
              cachedConsoleWidth = -1;
            }
          }
        });
  }

  protected final List<String> history = new ArrayList<String>();
  private final boolean historyEnabled;
  protected int cursorPosition = 0;
  protected int oldPromptLength = 0;
  protected int oldTextLength = 0;
  protected int oldCursorPosition = 0;
  protected int maxTotalLength = 0;
  protected String historyBuffer;

  protected Reader inStream;

  protected PrintStream outStream;
  protected OConsoleApplication console;

  public TTYConsoleReader(boolean historyEnabled) {
    this.historyEnabled = historyEnabled;
    File file = getHistoryFile(true);
    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(file));
      if (historyEnabled) {
        String historyEntry = reader.readLine();
        while (historyEntry != null) {
          history.add(historyEntry);
          historyEntry = reader.readLine();
        }
      }
      if (System.getProperty("file.encoding") != null) {
        inStream = new InputStreamReader(System.in, System.getProperty("file.encoding"));
        outStream = new PrintStream(System.out, false, System.getProperty("file.encoding"));
      } else {
        inStream = new InputStreamReader(System.in);
        outStream = System.out;
      }
    } catch (FileNotFoundException fnfe) {
      OLogManager.instance().error(this, "History file not found", fnfe);
    } catch (IOException ioe) {
      OLogManager.instance().error(this, "Error reading history file", ioe);
    }

    if (inStream == null)
      throw new OSystemException(
          "Cannot access to the input stream. Check permissions of running process");
  }

  public String readPassword() throws IOException {
    return readLine();
  }

  public String readLine() throws IOException {
    String consoleInput = "";

    StringBuffer buffer = new StringBuffer();
    cursorPosition = 0;
    historyBuffer = null;
    int historyNum = history.size();
    boolean hintedHistory = false;
    while (true) {
      boolean escape = false;
      boolean ctrl = false;
      int next = inStream.read();
      if (next == ESC) {
        escape = true;
        inStream.read();
        next = inStream.read();
      }
      if (escape) {
        if (next == 49) {
          inStream.read();
          next = inStream.read();
        }
        if (next == CTRL) {
          ctrl = true;
          next = inStream.read();
        }
        if (ctrl) {
          if (next == RIGHT_CHAR) {
            cursorPosition = buffer.indexOf(" ", cursorPosition) + 1;
            if (cursorPosition == 0) cursorPosition = buffer.length();
            updatePrompt(buffer);
          } else if (next == LEFT_CHAR) {
            if (cursorPosition > 1
                && cursorPosition < buffer.length()
                && buffer.charAt(cursorPosition - 1) == ' ') {
              cursorPosition = buffer.lastIndexOf(" ", (cursorPosition - 2)) + 1;
            } else {
              cursorPosition = buffer.lastIndexOf(" ", cursorPosition) + 1;
            }
            if (cursorPosition < 0) cursorPosition = 0;
            updatePrompt(buffer);
          }
        } else {
          if (next == UP_CHAR && !history.isEmpty()) {
            if (history.size() > 0) { // UP
              if (!hintedHistory
                  && (historyNum == history.size()
                      || !buffer.toString().equals(history.get(historyNum)))) {
                if (buffer.length() > 0) {
                  hintedHistory = true;
                  historyBuffer = buffer.toString();
                } else {
                  historyBuffer = null;
                }
              }
              historyNum = getHintedHistoryIndexUp(historyNum);
              if (historyNum > -1) {
                buffer = new StringBuffer(history.get(historyNum));
              } else {
                buffer = new StringBuffer(historyBuffer);
              }
              cursorPosition = buffer.length();
              updatePrompt(buffer);
            }
          } else if (next == DOWN_CHAR && !history.isEmpty()) { // DOWN
            if (history.size() > 0) {
              historyNum = getHintedHistoryIndexDown(historyNum);
              if (historyNum == history.size()) {
                if (historyBuffer != null) {
                  buffer = new StringBuffer(historyBuffer);
                } else {
                  buffer = new StringBuffer("");
                }
              } else {
                buffer = new StringBuffer(history.get(historyNum));
              }
              cursorPosition = buffer.length();
              updatePrompt(buffer);
            }
          } else if (next == RIGHT_CHAR) {
            if (cursorPosition < buffer.length()) {
              cursorPosition++;
              updatePrompt(buffer);
            }
          } else if (next == LEFT_CHAR) {
            if (cursorPosition > 0) {
              cursorPosition--;
              updatePrompt(buffer);
            }
          } else if (next == END_CHAR) {
            cursorPosition = buffer.length();
            updatePrompt(buffer);
          } else if (next == BEGIN_CHAR) {
            cursorPosition = 0;
            updatePrompt(buffer);
          }
        }
      } else {
        if (next == NEW_LINE_CHAR) {
          outStream.println();
          oldPromptLength = 0;
          oldTextLength = 0;
          oldCursorPosition = 0;
          maxTotalLength = 0;
          break;

        } else if (next == BACKSPACE_CHAR) {
          if (buffer.length() > 0 && cursorPosition > 0) {
            buffer.deleteCharAt(cursorPosition - 1);
            cursorPosition--;
            updatePrompt(buffer);
          }
        } else if (next == DEL_CHAR) {
          if (buffer.length() > 0 && cursorPosition >= 0 && cursorPosition < buffer.length()) {
            buffer.deleteCharAt(cursorPosition);
            updatePrompt(buffer);
          }
        } else if (next == HORIZONTAL_TAB_CHAR) {
          buffer = writeHint(buffer);
          cursorPosition = buffer.length();
          updatePrompt(buffer);
        } else {
          if ((next > UNIT_SEPARATOR_CHAR && next < BACKSPACE_CHAR) || next > BACKSPACE_CHAR) {
            if (cursorPosition == buffer.length()) {
              buffer.append((char) next);
            } else {
              buffer.insert(cursorPosition, (char) next);
            }
            cursorPosition++;
            updatePrompt(buffer);
          } else {
            outStream.println();
            outStream.print(buffer);
          }
        }
        historyNum = history.size();
        hintedHistory = false;
      }
    }
    consoleInput = buffer.toString();
    history.remove(consoleInput);
    history.add(consoleInput);
    historyNum = history.size();
    writeHistory(historyNum);

    if (consoleInput.equals("clear")) {
      outStream.flush();
      for (int i = 0; i < 150; i++) {
        outStream.println();
      }
      outStream.print("\r");
      outStream.print(console.getPrompt());
      return readLine();
    } else {
      return consoleInput;
    }
  }

  public void setConsole(OConsoleApplication iConsole) {
    console = iConsole;
  }

  @Override
  public int getConsoleWidth() {
    synchronized (signalLock) {
      if (cachedConsoleWidth > 0) // we have cached value
      return cachedConsoleWidth;

      if (cachedConsoleWidth == -1) { // no cached value
        try {
          final Process process =
              Runtime.getRuntime().exec(new String[] {"sh", "-c", "tput cols 2> /dev/tty"});
          final String line =
              new BufferedReader(new InputStreamReader(process.getInputStream())).readLine();
          process.waitFor();

          if (process.exitValue() == 0 && line != null) {
            cachedConsoleWidth = Integer.parseInt(line);
          } else cachedConsoleWidth = -2;
        } catch (IOException | NumberFormatException | InterruptedException ignore) {
          cachedConsoleWidth = -2;
        }
      }

      return cachedConsoleWidth == -2 || cachedConsoleWidth == 0
          ? OConsoleReader.FALLBACK_CONSOLE_WIDTH
          : cachedConsoleWidth;
    }
  }

  private void writeHistory(int historyNum) throws IOException {
    if (!historyEnabled) {
      return;
    }
    if (historyNum <= MAX_HISTORY_ENTRIES) {
      File historyFile = getHistoryFile(false);
      BufferedWriter writer = new BufferedWriter(new FileWriter(historyFile));
      try {
        for (String historyEntry : history) {
          writer.write(historyEntry);
          writer.newLine();
        }
      } finally {
        writer.flush();
        writer.close();
      }
    } else {
      File historyFile = getHistoryFile(false);
      BufferedWriter writer = new BufferedWriter(new FileWriter(historyFile));
      try {
        for (String historyEntry :
            history.subList(historyNum - MAX_HISTORY_ENTRIES - 1, historyNum - 1)) {
          writer.write(historyEntry);
          writer.newLine();
        }
      } finally {
        writer.flush();
        writer.close();
      }
    }
  }

  private StringBuffer writeHint(StringBuffer buffer) {
    List<String> suggestions = new ArrayList<String>();
    for (Method method : console.getConsoleMethods().keySet()) {
      String command = OConsoleApplication.getClearName(method.getName());
      if (command.startsWith(buffer.toString())) {
        suggestions.add(command);
      }
    }
    if (suggestions.size() > 1) {
      StringBuffer hintBuffer = new StringBuffer();
      String[] bufferComponents = buffer.toString().split(" ");
      String[] suggestionComponents;
      Set<String> bufferPart = new HashSet<String>();
      StringBuilder suggestionPart = null;
      boolean appendSpace = true;
      for (String suggestion : suggestions) {
        suggestionComponents = suggestion.split(" ");
        hintBuffer.append("* " + suggestion + " ");
        hintBuffer.append("\n");
        suggestionPart = new StringBuilder();
        if (bufferComponents.length == 0 || buffer.length() == 0) {
          suggestionPart = null;
        } else if (bufferComponents.length == 1) {
          bufferPart.add(suggestionComponents[0]);
          if (bufferPart.size() > 1) {
            suggestionPart = new StringBuilder(bufferComponents[0]);
            appendSpace = false;
          } else {
            suggestionPart = new StringBuilder(suggestionComponents[0]);
          }
        } else {
          bufferPart.add(suggestionComponents[bufferComponents.length - 1]);
          if (bufferPart.size() > 1) {
            for (int i = 0; i < bufferComponents.length; i++) {
              suggestionPart.append(bufferComponents[i]);
              if (i < (bufferComponents.length - 1)) {
                suggestionPart.append(" ");
              }
              appendSpace = false;
            }
          } else {
            for (int i = 0; i < suggestionComponents.length; i++) {
              suggestionPart.append(suggestionComponents[i]);
              suggestionPart.append(" ");
            }
          }
        }
      }
      if (suggestionPart != null) {
        buffer = new StringBuffer();
        buffer.append(suggestionPart);
        if (appendSpace) {
          buffer.append(" ");
        }
      }
      hintBuffer.append("-----------------------------\n");
      updateHints(hintBuffer);
    } else if (suggestions.size() > 0) {
      buffer = new StringBuffer();
      buffer.append(suggestions.get(0));
      buffer.append(" ");
    }
    return buffer;
  }

  private void updatePrompt(StringBuffer newText) {
    final int consoleWidth = getConsoleWidth();
    final String newPrompt = console.getPrompt();
    final int newTotalLength = newPrompt.length() + newText.length();
    final int oldTotalLength = oldPromptLength + oldTextLength;

    // 1. Hide the stream to reduce flickering.

    hideCursor();

    // 2. Position to the old prompt beginning.

    outStream.print("\r");
    moveCursorVertically(-getWraps(oldPromptLength, oldCursorPosition, consoleWidth));

    // 3. Write the new prompt over the old one.

    outStream.print(newPrompt);
    outStream.print(newText);

    // 4. Clear the remaining ending part of the old prompt, if any.

    final StringBuilder spaces = new StringBuilder();
    final int promptLengthDelta = oldTotalLength - newTotalLength;
    for (int i = 0; i < promptLengthDelta; i++) spaces.append(' ');
    outStream.print(spaces);

    // 5. Reset the stream to the prompt beginning. We may do navigation relative to the updated
    // console stream position,
    // but the math becomes too tricky in this case. Feel free to fix this ;)

    outStream.print("\r");
    if (newTotalLength > oldTotalLength) {
      if (newTotalLength > maxTotalLength && newTotalLength % consoleWidth == 0) {
        outStream.print('\n'); // make room for the stream
        moveCursorVertically(-1);
      }
      moveCursorVertically(-getWraps(newPrompt.length(), newText.length() - 1, consoleWidth));
    } else moveCursorVertically(-getWraps(oldPromptLength, oldTextLength - 1, consoleWidth));

    // 6. Place the stream at the right place.

    moveCursorVertically((newPrompt.length() + cursorPosition) / consoleWidth);
    moveCursorHorizontally((newPrompt.length() + cursorPosition) % consoleWidth);

    // 7. Update the stored things.

    oldPromptLength = newPrompt.length();
    oldTextLength = newText.length();
    oldCursorPosition = cursorPosition;
    maxTotalLength = Math.max(newTotalLength, maxTotalLength);

    // 8. Show the stream.

    showCursor();
  }

  private void erasePrompt() {
    final int consoleWidth = getConsoleWidth();
    final int oldTotalLength = oldPromptLength + oldTextLength;

    // 1. Hide the stream to reduce flickering.

    hideCursor();

    // 2. Position to the prompt beginning.

    outStream.print("\r");
    moveCursorVertically(-getWraps(oldPromptLength, oldCursorPosition, consoleWidth));

    // 3. Clear the prompt.

    final StringBuilder spaces = new StringBuilder();
    for (int i = 0; i < oldTotalLength; i++) spaces.append(' ');
    outStream.print(spaces);

    // 4. Reset the stream to the prompt beginning.

    outStream.print("\r");
    moveCursorVertically(-getWraps(oldPromptLength, oldCursorPosition, consoleWidth));

    // 5. Update the stored things.

    oldPromptLength = 0;
    oldTextLength = 0;
    oldCursorPosition = 0;
    maxTotalLength = 0;

    // 6. Show the stream.

    showCursor();
  }

  private void updateHints(StringBuffer hints) {
    erasePrompt();
    outStream.print("\r");
    outStream.print(hints);
  }

  private int getWraps(int promptLength, int cursorPosition, int consoleWidth) {
    return (promptLength + Math.max(0, cursorPosition)) / consoleWidth;
  }

  private void hideCursor() {
    outStream.print("\033[?25l");
  }

  private void showCursor() {
    outStream.print("\033[?25h");
  }

  private void moveCursorHorizontally(int delta) {
    if (delta > 0) outStream.print("\033[" + delta + "C");
    else if (delta < 0) outStream.print("\033[" + Math.abs(delta) + "D");
  }

  private void moveCursorVertically(int delta) {
    if (delta > 0) outStream.print("\033[" + delta + "B");
    else if (delta < 0) outStream.print("\033[" + Math.abs(delta) + "A");
  }

  private int getHintedHistoryIndexUp(int historyNum) {
    if (historyBuffer != null && !historyBuffer.equals("")) {
      for (int i = (historyNum - 1); i >= 0; i--) {
        if (history.get(i).startsWith(historyBuffer)) {
          return i;
        }
      }
      return -1;
    }
    return historyNum > 0 ? (historyNum - 1) : 0;
  }

  private int getHintedHistoryIndexDown(int historyNum) throws IOException {
    if (historyBuffer != null && !historyBuffer.equals("")) {
      for (int i = historyNum + 1; i < history.size(); i++) {
        if (history.get(i).startsWith(historyBuffer)) {
          return i;
        }
      }
      return history.size();
    }
    return historyNum < history.size() ? (historyNum + 1) : history.size();
  }

  private File getHistoryFile(boolean read) {

    final Path orientDBDir = Paths.get(System.getProperty("user.home"), ORIENTDB_HOME_DIR);
    try {
      Files.createDirectories(orientDBDir);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error creating OrientDB directory", e);
    }

    Path history = orientDBDir.resolve(HISTORY_FILE_NAME);
    try {

      if (!read) Files.deleteIfExists(history);

      if (Files.exists(history)) return history.toFile();

      return Files.createFile(history).toFile();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error creating history file", e);
    }

    return history.toFile();
  }
}
