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

import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.console.annotation.ConsoleParameter;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OArrays;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OConsoleApplication {
  protected static final String[]   COMMENT_PREFIXS = new String[] { "#", "--", "//" };
  protected final StringBuilder     commandBuffer   = new StringBuilder(2048);
  protected InputStream             in              = System.in;                       // System.in;
  protected PrintStream             out             = System.out;
  protected PrintStream             err             = System.err;
  protected String                  wordSeparator   = " ";
  protected String[]                helpCommands    = { "help", "?" };
  protected String[]                exitCommands    = { "exit", "bye", "quit" };
  protected Map<String, String>     properties      = new HashMap<String, String>();
  // protected OConsoleReader reader = new TTYConsoleReader();
  protected OConsoleReader          reader          = new DefaultConsoleReader();
  protected boolean                 interactiveMode;
  protected String[]                args;
  protected TreeMap<Method, Object> methods;

  protected enum RESULT {
    OK, ERROR, EXIT
  }

  public OConsoleApplication(String[] iArgs) {
    this.args = iArgs;
  }

  public static String getCorrectMethodName(Method m) {
    StringBuilder buffer = new StringBuilder(128);
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

  public void setReader(OConsoleReader iReader) {
    this.reader = iReader;
    reader.setConsole(this);
  }

  public int run() {
    interactiveMode = isInteractiveMode(args);
    onBefore();

    int result = 0;

    if (interactiveMode) {
      // EXECUTE IN INTERACTIVE MODE
      // final BufferedReader reader = new BufferedReader(new InputStreamReader(in));

      String consoleInput = null;

      while (true) {
        try {
          if (commandBuffer.length() == 0) {
            out.println();
            out.print(getPrompt());
          }

          consoleInput = reader.readLine();

          if (consoleInput == null || consoleInput.length() == 0)
            continue;

          if (!executeCommands(new ODFACommandStream(consoleInput), false))
            break;
        } catch (Exception e) {
          result = 1;
          OLogManager.instance().error(this, "Error on reading console input: %s", e, consoleInput);
        }
      }
    } else {
      // EXECUTE IN BATCH MODE
      result = executeBatch(getCommandLine(args)) ? 0 : 1;
    }

    onAfter();

    return result;
  }

  public void message(final String iMessage, final Object... iArgs) {
    final int verboseLevel = getVerboseLevel();
    if (verboseLevel > 1)
      out.printf(iMessage, iArgs);
  }

  public void error(final String iMessage, final Object... iArgs) {
    final int verboseLevel = getVerboseLevel();
    if (verboseLevel > 0)
      out.printf(iMessage, iArgs);
  }

  public int getVerboseLevel() {
    final String v = properties.get("verbose");
    final int verboseLevel = v != null ? Integer.parseInt(v) : 2;
    return verboseLevel;
  }

  public boolean isEchoEnabled() {
    return isPropertyEnabled("echo");
  }

  protected boolean isPropertyEnabled(final String iPropertyName) {
    String v = properties.get(iPropertyName);
    if (v != null) {
      v = v.toLowerCase();
      return v.equals("true") || v.equals("on");
    }
    return false;
  }

  protected String getPrompt() {
    return String.format("%s> ", getContext());
  }

  protected String getContext() {
    return "";
  }

  protected boolean isInteractiveMode(String[] args) {
    return args.length == 0;
  }

  protected boolean executeBatch(final String commandLine) {
    final File commandFile = new File(commandLine);

    OCommandStream scanner;
    try {
      scanner = new ODFACommandStream(commandFile);
    } catch (FileNotFoundException e) {
      scanner = new ODFACommandStream(commandLine);
    }

    return executeCommands(scanner, true);
  }

  protected boolean executeCommands(final OCommandStream commandStream, final boolean iBatchMode) {
    try {
      while (commandStream.hasNext()) {
        String commandLine = commandStream.nextCommand();

        if (commandLine.isEmpty())
          // EMPTY LINE
          continue;

        if (isComment(commandLine))
          continue;

        // SCRIPT CASE: MANAGE ENSEMBLING ALL TOGETHER
        if (isCollectingCommands(commandLine)) {
          // BEGIN: START TO COLLECT
          out.println("[Started multi-line command. Type just 'end' to finish and execute]");
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
          if (iBatchMode || isEchoEnabled()) {
            out.println();
            out.print(getPrompt());
            out.print(commandLine);
            out.println();
          }

          final RESULT status = execute(commandLine);
          commandLine = null;

          if (status == RESULT.EXIT || (status == RESULT.ERROR && !Boolean.parseBoolean(properties.get("ignoreErrors")))
              && iBatchMode)
            return false;
        }
      }

      if (commandBuffer.length() == 0) {
        if (commandBuffer.length() > 0) {
          if (iBatchMode) {
            out.println();
            out.print(getPrompt());
            out.print(commandBuffer);
            out.println();
          }

          final RESULT status = execute(commandBuffer.toString());
          if (status == RESULT.EXIT || (status == RESULT.ERROR && !Boolean.parseBoolean(properties.get("ignoreErrors")))
              && iBatchMode)
            return false;
        }
      }
    } finally {
      commandStream.close();
    }
    return true;
  }

  protected boolean isComment(final String commandLine) {
    for (String comment : COMMENT_PREFIXS)
      if (commandLine.startsWith(comment))
        return true;
    return false;
  }

  protected boolean isCollectingCommands(final String iLine) {
    return false;
  }

  protected RESULT execute(String iCommand) {
    iCommand = iCommand.trim();

    if (iCommand.length() == 0)
      // NULL LINE: JUMP IT
      return RESULT.OK;

    if (isComment(iCommand))
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
    final StringBuilder lastCommandInvoked = new StringBuilder(1024);

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
        final int actualParamCount = commandWords.length - commandWordCount;
        if (m.getParameterTypes().length > actualParamCount) {
          // METHOD PARAMS AND USED PARAMS MISMATCH: CHECK FOR OPTIONALS
          for (int paramNum = m.getParameterAnnotations().length - 1; paramNum > actualParamCount - 1; paramNum--) {
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
        if (e.getCause() != null)
          onException(e.getCause());
        else
          e.printStackTrace(err);
        return RESULT.ERROR;
      }
      return RESULT.OK;
    }

    if (lastMethodInvoked != null)
      syntaxError(lastCommandInvoked.toString(), lastMethodInvoked);

    error("\n!Unrecognized command: '%s'", iCommand);
    return RESULT.ERROR;
  }

  protected void syntaxError(String iCommand, Method m) {
    error(
        "\n!Wrong syntax. If you're using a file make sure all commands are delimited by semicolon (;) or a linefeed (\\n)\n\r\n\r Expected: %s ",
        iCommand);

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
        message("[<%s>] ", paramName);
      else
        message("<%s> ", paramName);

      buffer.append("* ");
      buffer.append(String.format("%-15s", paramName));

      if (paramDescription != null)
        buffer.append(String.format("%-15s", paramDescription));
      buffer.append("\n");
    }

    message(buffer.toString());
  }

  /**
   * Returns a map of all console method and the object they can be called on.
   *
   * @return Map&lt;Method,Object&gt;
   */
  protected Map<Method, Object> getConsoleMethods() {
    if (methods != null)
      return methods;

    // search for declared command collections
    final Iterator<OConsoleCommandCollection> ite = ServiceLoader.load(OConsoleCommandCollection.class).iterator();
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

    methods = new TreeMap<Method, Object>(new Comparator<Method>() {
      public int compare(Method o1, Method o2) {
        final ConsoleCommand ann1 = o1.getAnnotation(ConsoleCommand.class);
        final ConsoleCommand ann2 = o2.getAnnotation(ConsoleCommand.class);

        if (ann1 != null && ann2 != null) {
          if (ann1.priority() != ann2.priority())
            // PRIORITY WINS
            return ann1.priority() - ann2.priority();
        }

        int res = o1.getName().compareTo(o2.getName());
        if (res == 0)
          res = o1.toString().compareTo(o2.toString());
        return res;
      }
    });

    for (final Object candidate : candidates) {
      final Method[] classMethods = candidate.getClass().getMethods();

      for (Method m : classMethods) {
        if (Modifier.isAbstract(m.getModifiers()) || Modifier.isStatic(m.getModifiers()) || !Modifier.isPublic(m.getModifiers())) {
          continue;
        }
        if (m.getReturnType() != Void.TYPE) {
          continue;
        }
        methods.put(m, candidate);
      }
    }
    return methods;
  }

  protected Map<String, Object> addCommand(Map<String, Object> commandsTree, String commandLine) {
    return commandsTree;
  }

  protected void help() {
    message("\nAVAILABLE COMMANDS:\n");

    for (Method m : getConsoleMethods().keySet()) {
      com.orientechnologies.common.console.annotation.ConsoleCommand annotation = m
          .getAnnotation(com.orientechnologies.common.console.annotation.ConsoleCommand.class);

      if (annotation == null)
        continue;

      message("* %-70s%s\n", getCorrectMethodName(m), annotation.description());
    }
    message("* %-70s%s\n", getClearName("help"), "Print this help");
    message("* %-70s%s\n", getClearName("exit"), "Close the console");

  }

  protected String getCommandLine(String[] iArguments) {
    StringBuilder command = new StringBuilder(512);
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

  protected void onException(final Throwable throwable) {
    throwable.printStackTrace(err);
  }

  public void setOutput(PrintStream iOut) {
    this.out = iOut;
  }

}
