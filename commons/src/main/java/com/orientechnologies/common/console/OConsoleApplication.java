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
package com.orientechnologies.common.console;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.spi.ServiceRegistry;

import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OArrays;

public class OConsoleApplication {
  protected enum RESULT {
    OK, ERROR, EXIT
  };

  protected InputStream         in               = System.in;                    // System.in;
  protected PrintStream         out              = System.out;
  protected PrintStream         err              = System.err;

  protected String              lineSeparator    = "\n";
  protected String              commandSeparator = ";";
  protected String              wordSeparator    = " ";
  protected String[]            helpCommands     = { "help", "?" };
  protected String[]            exitCommands     = { "exit", "bye", "quit" };

  protected Map<String, String> properties       = new HashMap<String, String>();

  // protected OConsoleReader reader = new TTYConsoleReader();
  protected OConsoleReader      reader           = new DefaultConsoleReader();
  protected boolean             interactiveMode;
  protected String[]            args;

  protected static final String COMMENT_PREFIX   = "#";

  public void setReader(OConsoleReader iReader) {
    this.reader = iReader;
    reader.setConsole(this);
  }

  public OConsoleApplication(String[] iArgs) {
    this.args = iArgs;
  }

  public int run() {
    interactiveMode = isInteractiveMode(args);
    onBefore();

    int result = 0;

    if (interactiveMode) {
      // EXECUTE IN INTERACTIVE MODE
      // final BufferedReader reader = new BufferedReader(new InputStreamReader(in));

      String consoleInput;

      while (true) {
        out.println();
        out.print("orientdb> ");
        consoleInput = reader.readLine();

        if (consoleInput == null || consoleInput.length() == 0)
          continue;

        if (!executeCommands(new Scanner(consoleInput), false))
          break;
      }
    } else {
      // EXECUTE IN BATCH MODE
      result = executeBatch(getCommandLine(args)) ? 0 : 1;
    }

    onAfter();

    return result;
  }

  protected boolean isInteractiveMode(String[] args) {
    return args.length == 0;
  }

  protected boolean executeBatch(final String commandLine) {
    final File commandFile = new File(commandLine);

    Scanner scanner = null;

    try {
      scanner = new Scanner(commandFile);
    } catch (FileNotFoundException e) {
      scanner = new Scanner(commandLine);
    }

    return executeCommands(scanner, true);
  }

  protected boolean executeCommands(final Scanner iScanner, final boolean iExitOnException) {
    final StringBuilder commandBuffer = new StringBuilder();

    try {
      String commandLine = null;

      iScanner.useDelimiter(commandSeparator);
      while (iScanner.hasNext()) {

        commandLine = iScanner.next().trim();

        if (commandLine.startsWith("--") || commandLine.startsWith("//"))
          // SKIP COMMENTS
          continue;

        // JS CASE: MANAGE ENSEMBLING ALL TOGETHER
        if (commandLine.startsWith("js")) {
          // BEGIN: START TO COLLECT
          commandBuffer.append(commandLine);
          commandLine = null;
        } else if (commandLine.startsWith("end") && commandBuffer.length() > 0) {
          // END: FLUSH IT
          commandLine = commandBuffer.toString();
          commandBuffer.setLength(0);

        } else if (commandBuffer.length() > 0) {
          // BUFFER IT
          commandBuffer.append(';');
          commandBuffer.append(commandLine);
          commandLine = null;
        }

        if (commandLine != null) {
          final RESULT status = execute(commandLine);
          commandLine = null;

          if (status == RESULT.EXIT || status == RESULT.ERROR && iExitOnException)
            return false;
        }
      }

      if (commandBuffer.length() > 0) {
        final RESULT status = execute(commandBuffer.toString());
        if (status == RESULT.EXIT || status == RESULT.ERROR && iExitOnException)
          return false;
      }
    } finally {
      iScanner.close();
    }
    return true;
  }

  protected RESULT execute(String iCommand) {
    iCommand = iCommand.trim();

    if (iCommand.length() == 0)
      // NULL LINE: JUMP IT
      return RESULT.OK;

    if (iCommand.startsWith(COMMENT_PREFIX))
      // COMMENT: JUMP IT
      return RESULT.OK;

    String[] commandWords = OStringParser.getWords(iCommand, wordSeparator);

    for (String cmd : helpCommands)
      if (cmd.equals(commandWords[0])) {
        help();
        return RESULT.OK;
      }

    for (String cmd : exitCommands)
      if (cmd.equals(commandWords[0])) {
        return RESULT.EXIT;
      }

    Method lastMethodInvoked = null;
    final StringBuilder lastCommandInvoked = new StringBuilder();

    final String commandLowerCase = iCommand.toLowerCase();

    for (Entry<Method, Object> entry : getConsoleMethods().entrySet()) {
      final Method m = entry.getKey();
      final String methodName = m.getName();
      final ConsoleCommand ann = m.getAnnotation(ConsoleCommand.class);

      final StringBuilder commandName = new StringBuilder();
      char ch;
      int commandWordCount = 1;
      for (int i = 0; i < methodName.length(); ++i) {
        ch = methodName.charAt(i);
        if (Character.isUpperCase(ch)) {
          commandName.append(" ");
          ch = Character.toLowerCase(ch);
          commandWordCount++;
        }
        commandName.append(ch);
      }

      if (!commandLowerCase.equals(commandName.toString()) && !commandLowerCase.startsWith(commandName.toString() + " ")) {
        if (ann == null)
          continue;

        String[] aliases = ann.aliases();
        if (aliases == null || aliases.length == 0)
          continue;

        boolean aliasMatch = false;
        for (String alias : aliases) {
          if (iCommand.startsWith(alias.split(" ")[0])) {
            aliasMatch = true;
            commandWordCount = 1;
            break;
          }
        }

        if (!aliasMatch)
          continue;
      }

      Object[] methodArgs;

      // BUILD PARAMETERS
      if (ann != null && !ann.splitInWords()) {
        methodArgs = new String[] { iCommand.substring(iCommand.indexOf(' ') + 1) };
      } else {
        if (m.getParameterTypes().length > commandWords.length - commandWordCount) {
          // METHOD PARAMS AND USED PARAMS MISMATCH: CHECK FOR OPTIONALS
          for (int paramNum = m.getParameterAnnotations().length - 1; paramNum > -1; paramNum--) {
            final Annotation[] paramAnn = m.getParameterAnnotations()[paramNum];
            if (paramAnn != null)
              for (int annNum = paramAnn.length - 1; annNum > -1; annNum--) {
                if (paramAnn[annNum] instanceof ConsoleParameter) {
                  final ConsoleParameter annotation = (ConsoleParameter) paramAnn[annNum];
                  if (annotation.optional())
                    commandWords = OArrays.copyOf(commandWords, commandWords.length + 1);
                  break;
                }
              }
          }
        }
        methodArgs = OArrays.copyOfRange(commandWords, commandWordCount, commandWords.length);
      }

      try {
        m.invoke(entry.getValue(), methodArgs);

      } catch (IllegalArgumentException e) {
        lastMethodInvoked = m;
        // GET THE COMMAND NAME
        lastCommandInvoked.setLength(0);
        for (int i = 0; i < commandWordCount; ++i) {
          if (lastCommandInvoked.length() > 0)
            lastCommandInvoked.append(" ");
          lastCommandInvoked.append(commandWords[i]);
        }
        continue;
      } catch (Exception e) {
        // e.printStackTrace();
        // err.println();
        if (e.getCause() != null)
          onException(e.getCause());
        else
          e.printStackTrace();
        return RESULT.ERROR;
      }
      return RESULT.OK;
    }

    if (lastMethodInvoked != null)
      syntaxError(lastCommandInvoked.toString(), lastMethodInvoked);

    out.println("!Unrecognized command: '" + iCommand + "'");
    return RESULT.ERROR;
  }

  protected void syntaxError(String iCommand, Method m) {
    out.print("!Wrong syntax. If you're using a file make sure all commands are delimited by ';'\n\r\n\r Expected: " + iCommand
        + " ");

    String paramName = null;
    String paramDescription = null;
    boolean paramOptional = false;

    StringBuilder buffer = new StringBuilder("\n\nWhere:\n\n");
    for (Annotation[] annotations : m.getParameterAnnotations()) {
      for (Annotation ann : annotations) {
        if (ann instanceof com.orientechnologies.common.console.annotation.ConsoleParameter) {
          paramName = ((com.orientechnologies.common.console.annotation.ConsoleParameter) ann).name();
          paramDescription = ((com.orientechnologies.common.console.annotation.ConsoleParameter) ann).description();
          paramOptional = ((com.orientechnologies.common.console.annotation.ConsoleParameter) ann).optional();
          break;
        }
      }

      if (paramName == null)
        paramName = "?";

      if (paramOptional)
        out.print("[<" + paramName + ">] ");
      else
        out.print("<" + paramName + "> ");

      buffer.append("* ");
      buffer.append(String.format("%-15s", paramName));

      if (paramDescription != null)
        buffer.append(String.format("%-15s", paramDescription));
      buffer.append("\n");
    }

    out.println(buffer);
  }

  /**
   * Returns a map of all console method and the object they can be called on.
   * 
   * @return Map<Method,Object>
   */
  protected Map<Method, Object> getConsoleMethods() {

    // search for declared command collections
    final Iterator<OConsoleCommandCollection> ite = ServiceRegistry.lookupProviders(OConsoleCommandCollection.class);
    final Collection<Object> candidates = new ArrayList<Object>();
    candidates.add(this);
    while (ite.hasNext()) {
      try {
        // make a copy and set it's context
        final OConsoleCommandCollection cc = ite.next().getClass().newInstance();
        cc.setContext(this);
        candidates.add(cc);
      } catch (InstantiationException ex) {
        Logger.getLogger(OConsoleApplication.class.getName()).log(Level.WARNING, ex.getMessage());
      } catch (IllegalAccessException ex) {
        Logger.getLogger(OConsoleApplication.class.getName()).log(Level.WARNING, ex.getMessage());
      }
    }

    final Map<Method, Object> consoleMethods = new TreeMap<Method, Object>(new Comparator<Method>() {
      public int compare(Method o1, Method o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    for (final Object candidate : candidates) {
      final Method[] methods = candidate.getClass().getMethods();

      for (Method m : methods) {
        if (Modifier.isAbstract(m.getModifiers()) || Modifier.isStatic(m.getModifiers()) || !Modifier.isPublic(m.getModifiers())) {
          continue;
        }
        if (m.getReturnType() != Void.TYPE) {
          continue;
        }
        consoleMethods.put(m, candidate);
      }
    }
    return consoleMethods;
  }

  protected Map<String, Object> addCommand(Map<String, Object> commandsTree, String commandLine) {
    return commandsTree;
  }

  protected void help() {
    out.println();
    out.println("AVAILABLE COMMANDS:");
    out.println();

    for (Method m : getConsoleMethods().keySet()) {
      com.orientechnologies.common.console.annotation.ConsoleCommand annotation = m
          .getAnnotation(com.orientechnologies.common.console.annotation.ConsoleCommand.class);

      if (annotation == null)
        continue;

      System.out.print(String.format("* %-70s%s\n", getCorrectMethodName(m), annotation.description()));
    }
    System.out.print(String.format("* %-70s%s\n", getClearName("help"), "Print this help"));
    System.out.print(String.format("* %-70s%s\n", getClearName("exit"), "Close the console"));

  }

  public static String getCorrectMethodName(Method m) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(getClearName(m.getName()));
    for (int i = 0; i < m.getParameterAnnotations().length; i++) {
      for (int j = 0; j < m.getParameterAnnotations()[i].length; j++) {
        if (m.getParameterAnnotations()[i][j] instanceof com.orientechnologies.common.console.annotation.ConsoleParameter) {
          buffer
              .append(" <"
                  + ((com.orientechnologies.common.console.annotation.ConsoleParameter) m.getParameterAnnotations()[i][j]).name()
                  + ">");
        }
      }
    }
    return buffer.toString();
  }

  public static String getClearName(String iJavaName) {
    StringBuilder buffer = new StringBuilder();

    char c;
    if (iJavaName != null) {
      buffer.append(iJavaName.charAt(0));
      for (int i = 1; i < iJavaName.length(); ++i) {
        c = iJavaName.charAt(i);

        if (Character.isUpperCase(c)) {
          buffer.append(' ');
        }

        buffer.append(Character.toLowerCase(c));
      }

    }
    return buffer.toString();
  }

  protected String getCommandLine(String[] iArguments) {
    StringBuilder command = new StringBuilder();
    for (int i = 0; i < iArguments.length; ++i) {
      if (i > 0)
        command.append(" ");

      command.append(iArguments[i]);
    }
    return command.toString();
  }

  protected void onBefore() {
  }

  protected void onAfter() {
  }

  protected void onException(Throwable throwable) {
    throwable.printStackTrace();
  }
}
