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

import java.io.Console;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.console.annotation.ConsoleCommand;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OArrays;

public class ConsoleApplication {

	protected InputStream					in								= System.in;
	protected PrintStream					out								= System.out;
	protected PrintStream					err								= System.err;
	protected String							commandSeparator	= ";";
	protected String							wordSeparator			= " ";
	protected String[]						helpCommands			= { "help", "?" };
	protected String[]						exitCommands			= { "exit", "bye", "quit" };
	protected Map<String, Object>	properties				= new HashMap<String, Object>();

	public ConsoleApplication(String[] args) {
		boolean interactiveMode = isInteractiveMode(args);

		onBefore();

		if (interactiveMode) {
			// EXECUTE IN INTERACTIVE MODE
			Console console = System.console();
			if (console == null) {
				err.println("Error on reading from the console");
				return;
			}

			String consoleInput;

			while (true) {
				out.println();
				out.print("> ");
				consoleInput = console.readLine();

				if (consoleInput == null || consoleInput.length() == 0)
					continue;

				if (!executeCommands(consoleInput))
					break;
			}
		} else {
			// EXECUTE IN BATCH MODE
			executeCommands(getCommandLine(args));
		}

		onAfter();
	}

	protected boolean isInteractiveMode(String[] args) {
		return args.length == 0;
	}

	protected boolean executeCommands(String iCommands) {
		String[] commandLines = iCommands.split(commandSeparator);
		for (String commandLine : commandLines)
			if (!execute(commandLine))
				return false;
		return true;
	}

	protected boolean execute(String iCommand) {
		iCommand = iCommand.trim();
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
		StringBuilder lastCommandInvoked = new StringBuilder();

		for (Method m : getConsoleMethods()) {
			methodName = m.getName();
			ann = m.getAnnotation(ConsoleCommand.class);

			StringBuilder commandName = new StringBuilder();
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

			if (!iCommand.startsWith(commandName.toString())) {
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
				methodArgs = new String[] { iCommand.substring(iCommand.indexOf(" ") + 1) };
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

		out.println("!Unrecognized command: " + iCommand);
		return true;
	}

	protected void syntaxError(String iCommand, Method m) {
		out.print("!Wrong syntax. Expected: " + iCommand + " ");

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
		final Method[] methods = getClass().getDeclaredMethods();

		final List<Method> consoleMethods = new ArrayList<Method>();

		for (Method m : methods) {
			if (Modifier.isAbstract(m.getModifiers()) || Modifier.isStatic(m.getModifiers()) || !Modifier.isPublic(m.getModifiers()))
				continue;

			if (m.getReturnType() != Void.TYPE)
				continue;

			consoleMethods.add(m);
		}

		return consoleMethods;
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

			System.out.print(String.format("* %-20s%s\n", getClearName(m.getName()), annotation.description()));
		}
		System.out.print(String.format("* %-20s%s\n", getClearName("help"), "Print this help"));
		System.out.print(String.format("* %-20s%s\n", getClearName("exit"), "Close the console"));

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
