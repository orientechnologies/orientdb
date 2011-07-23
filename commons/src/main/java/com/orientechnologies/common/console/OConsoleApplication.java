/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OArrays;

public class OConsoleApplication {
	protected InputStream					in								= System.in;											// System.in;
	protected PrintStream					out								= System.out;
	protected PrintStream					err								= System.err;

	protected char								commandSeparator	= ';';
	protected String							wordSeparator			= " ";
	protected String[]						helpCommands			= { "help", "?" };
	protected String[]						exitCommands			= { "exit", "bye", "quit" };

	protected Map<String, Object>	properties				= new HashMap<String, Object>();

	// protected OConsoleReader reader = new TTYConsoleReader();
	protected OConsoleReader			reader						= new DefaultConsoleReader();
	protected boolean							interactiveMode;
	protected String[]						args;

	public void setReader(OConsoleReader iReader) {
		this.reader = iReader;
		reader.setConsole(this);
	}

	public OConsoleApplication(String[] iArgs) {
		this.args = iArgs;
	}

	public void run() {
		interactiveMode = isInteractiveMode(args);
		onBefore();

		if (interactiveMode) {
			// EXECUTE IN INTERACTIVE MODE
			// final BufferedReader reader = new BufferedReader(new InputStreamReader(in));

			String consoleInput;

			while (true) {
				out.println();
				out.print("> ");
				consoleInput = reader.readLine();

				if (consoleInput == null || consoleInput.length() == 0)
					continue;

				if (!executeCommands(consoleInput))
					break;
			}
		} else {
			// EXECUTE IN BATCH MODE
			executeBatch(getCommandLine(args));
		}

		onAfter();
	}

	protected boolean isInteractiveMode(String[] args) {
		return args.length == 0;
	}

	protected boolean executeBatch(final String commandLine) {
		final File commandFile = new File(commandLine);
		if (commandFile.exists()) {
			try {
				return executeCommands(new Scanner(commandFile).useDelimiter(";"));
			} catch (FileNotFoundException fnfe) {
				return false;
			}
		} else {
			return executeCommands(commandLine);
		}
	}

	protected boolean executeCommands(final String iCommands) {
		String[] commandLines = OStringParser.split(iCommands, commandSeparator, OStringParser.COMMON_JUMP);
		// String[] commandLines = iCommands.split(commandSeparator);
		for (String commandLine : commandLines)
			if (!execute(commandLine))
				return false;
		return true;
	}

	protected boolean executeCommands(final Scanner iScanner) {
		while (iScanner.hasNext()) {
			String commandLine = iScanner.next();
			if (!execute(commandLine))
				return false;
		}
		return true;
	}

	protected boolean execute(String iCommand) {
		iCommand = iCommand.trim();

		if (iCommand.length() == 0)
			// NULL LINE: JUMP IT
			return true;

		final String[] commandWords = OStringParser.getWords(iCommand, wordSeparator);

		for (String cmd : helpCommands)
			if (cmd.equals(commandWords[0])) {
				help();
				return true;
			}

		for (String cmd : exitCommands)
			if (cmd.equals(commandWords[0])) {
				return false;
			}

		String methodName;
		ConsoleCommand ann;
		Method lastMethodInvoked = null;
		final StringBuilder lastCommandInvoked = new StringBuilder();

		final String commandLowerCase = iCommand.toLowerCase();

		for (Method m : getConsoleMethods()) {
			methodName = m.getName();
			ann = m.getAnnotation(ConsoleCommand.class);

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

			if (!commandLowerCase.startsWith(commandName.toString())) {
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
				methodArgs = OArrays.copyOfRange(commandWords, commandWordCount, commandWords.length);
			}

			try {
				m.invoke(this, methodArgs);

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
			}
			return true;
		}

		if (lastMethodInvoked != null)
			syntaxError(lastCommandInvoked.toString(), lastMethodInvoked);

		out.println("!Unrecognized command: '" + iCommand + "'");
		return true;
	}

	protected void syntaxError(String iCommand, Method m) {
		out.print("!Wrong syntax. If you're using a file make sure all commands are delimited by ';'\n\r\n\r Expected: " + iCommand
				+ " ");

		String paramName = null;
		String paramDescription = null;

		StringBuilder buffer = new StringBuilder("\n\nWhere:\n\n");
		for (Annotation[] annotations : m.getParameterAnnotations()) {
			for (Annotation ann : annotations) {
				if (ann instanceof com.orientechnologies.common.console.annotation.ConsoleParameter) {
					paramName = ((com.orientechnologies.common.console.annotation.ConsoleParameter) ann).name();
					paramDescription = ((com.orientechnologies.common.console.annotation.ConsoleParameter) ann).description();
					break;
				}
			}

			if (paramName == null)
				paramName = "?";

			out.print("<" + paramName + "> ");

			buffer.append("* ");
			buffer.append(String.format("%-15s", paramName));

			if (paramDescription != null)
				buffer.append(String.format("%-15s", paramDescription));
			buffer.append("\n");
		}

		out.println(buffer);
	}

	protected List<Method> getConsoleMethods() {
		final Method[] methods = getClass().getMethods();

		final List<Method> consoleMethods = new ArrayList<Method>();

		for (Method m : methods) {
			if (Modifier.isAbstract(m.getModifiers()) || Modifier.isStatic(m.getModifiers()) || !Modifier.isPublic(m.getModifiers()))
				continue;

			if (m.getReturnType() != Void.TYPE)
				continue;

			Collections.sort(consoleMethods, new Comparator<Method>() {
				public int compare(Method o1, Method o2) {
					return o1.getName().compareTo(o2.getName());
				}
			});
			consoleMethods.add(m);
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

		for (Method m : getConsoleMethods()) {
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
