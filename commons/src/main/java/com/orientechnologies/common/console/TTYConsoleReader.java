package com.orientechnologies.common.console;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TTYConsoleReader implements OConsoleReader {

	public static int							DOWN_CHAR						= 66;

	public static int							UP_CHAR							= 65;

	public static int							HORIZONTAL_TAB_CHAR	= 9;

	public static int							VERTICAL_TAB_CHAR		= 11;

	public static int							DEL_CHAR						= 127;

	public static int							NEW_LINE_CHAR				= 10;

	public static int							UNIT_SEPARATOR_CHAR	= 31;

	protected List<String>				history							= new ArrayList<String>();

	protected OConsoleApplication	console;

	public String readLine() {
		String consoleInput = "";
		try {
			StringBuffer buffer = new StringBuffer();
			int historyNum = history.size();
			while (true) {

				boolean escape = false;
				int next = System.in.read();
				if (next == 27) {
					escape = true;
					System.in.read();
					next = System.in.read();
				}
				if (escape) {
					if (next == UP_CHAR && !history.isEmpty()) {
						if (history.size() > 0) { // UP
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							rewriteConsole(cleaner);
							historyNum = historyNum > 0 ? historyNum - 1 : 0;
							buffer = new StringBuffer(history.get(historyNum));
							rewriteConsole(buffer);
							// writeHistory(historyNum);
						}
					}
					if (next == DOWN_CHAR && !history.isEmpty()) { // DOWN
						if (history.size() > 0) {
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							rewriteConsole(cleaner);

							historyNum = historyNum < history.size() ? historyNum + 1 : history.size();
							if (historyNum == history.size()) {
								buffer = new StringBuffer("");
							} else {
								buffer = new StringBuffer(history.get(historyNum));
							}
							rewriteConsole(buffer);
							// writeHistory(historyNum);
						}
					} else {
					}
				} else {
					if (next == NEW_LINE_CHAR) {
						System.out.println();
						break;
					} else if (next == DEL_CHAR) {
						if (buffer.length() > 0) {
							buffer.deleteCharAt(buffer.length() - 1);
							StringBuffer cleaner = new StringBuffer(buffer);
							cleaner.append(" ");
							rewriteConsole(cleaner);
							rewriteConsole(buffer);
						}
					} else if (next == HORIZONTAL_TAB_CHAR) {
						StringBuffer cleaner = new StringBuffer();
						for (int i = 0; i < buffer.length(); i++) {
							cleaner.append(" ");
						}
						buffer = writeHint(buffer);
						rewriteConsole(cleaner);
						rewriteConsole(buffer);
					} else {
						if (next > UNIT_SEPARATOR_CHAR && next < DEL_CHAR) {
							System.out.print((char) next);
							buffer.append((char) next);
						} else {
							System.out.println();
							System.out.print(buffer);
						}
					}
					historyNum = history.size();
				}
			}
			consoleInput = buffer.toString();
			history.remove(consoleInput);
			history.add(consoleInput);
			historyNum = history.size();
		} catch (IOException e) {
			return null;
		}
		return consoleInput;
	}

	private StringBuffer writeHint(StringBuffer buffer) {
		List<String> suggestions = new ArrayList<String>();
		for (Method method : console.getConsoleMethods()) {
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
			String suggestionPart = null;
			boolean appendSpace = true;
			for (String suggestion : suggestions) {
				suggestionComponents = suggestion.split(" ");
				hintBuffer.append("* " + suggestion + " ");
				hintBuffer.append("\n");
				suggestionPart = "";
				if (bufferComponents.length == 0 || buffer.length() == 0) {
					suggestionPart = null;
				} else if (bufferComponents.length == 1) {
					bufferPart.add(suggestionComponents[0]);
					if (bufferPart.size() > 1) {
						suggestionPart = bufferComponents[0];
						appendSpace = false;
					} else {
						suggestionPart = suggestionComponents[0];
					}
				} else {
					bufferPart.add(suggestionComponents[bufferComponents.length - 1]);
					if (bufferPart.size() > 1) {
						for (int i = 0; i < bufferComponents.length; i++) {
							suggestionPart += bufferComponents[i];
							if (i < (bufferComponents.length - 1)) {
								suggestionPart += " ";
							}
							appendSpace = false;
						}
					} else {
						for (int i = 0; i < suggestionComponents.length; i++) {
							suggestionPart += suggestionComponents[i] + " ";
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
			rewriteHintConsole(hintBuffer);
		} else if (suggestions.size() > 0) {
			buffer = new StringBuffer();
			buffer.append(suggestions.get(0));
			buffer.append(" ");
		}
		return buffer;
	}

	public void setConsole(OConsoleApplication iConsole) {
		console = iConsole;
	}

	public OConsoleApplication getConsole() {
		return console;
	}

	private void rewriteConsole(StringBuffer buffer) {
		System.out.print("\r");
		System.out.print("> ");
		System.out.print(buffer);
	}

	private void rewriteHintConsole(StringBuffer buffer) {
		System.out.print("\r");
		System.out.print(buffer);
	}

}
