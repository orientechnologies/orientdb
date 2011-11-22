package com.orientechnologies.common.console;

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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;

public class TTYConsoleReader implements OConsoleReader {

	private static final String	HISTORY_FILE_NAME		= ".orientdb_history";

	private static int					MAX_HISTORY_ENTRIES	= 50;

	public static int						END_CHAR						= 70;

	public static int						BEGIN_CHAR					= 72;

	public static int						DEL_CHAR						= 126;

	public static int						DOWN_CHAR						= 66;

	public static int						UP_CHAR							= 65;

	public static int						RIGHT_CHAR					= 67;

	public static int						LEFT_CHAR						= 68;

	public static int						HORIZONTAL_TAB_CHAR	= 9;

	public static int						VERTICAL_TAB_CHAR		= 11;

	public static int						BACKSPACE_CHAR			= 127;

	public static int						NEW_LINE_CHAR				= 10;

	public static int						UNIT_SEPARATOR_CHAR	= 31;

	protected int								currentPos					= 0;

	protected List<String>			history							= new ArrayList<String>();

	protected String						historyBuffer;

	protected Reader						inStream;

	protected PrintStream				outStream;

	public TTYConsoleReader() {
		File file = getHistoryFile(true);
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(file));
			String historyEntry = reader.readLine();
			while (historyEntry != null) {
				history.add(historyEntry);
				historyEntry = reader.readLine();
			}
			if (System.getProperty("file.encoding") != null) {
				inStream = new InputStreamReader(System.in, System.getProperty("file.encoding"));
				outStream = new PrintStream(System.out, true, System.getProperty("file.encoding"));
			} else {
				inStream = new InputStreamReader(System.in);
				outStream = System.out;
			}
		} catch (FileNotFoundException fnfe) {
			OLogManager.instance().error(this, "History file not found", fnfe, "");
		} catch (IOException ioe) {
			OLogManager.instance().error(this, "Error reading history file.", ioe, "");
		}
	}

	protected OConsoleApplication	console;

	public String readLine() {
		String consoleInput = "";
		try {
			StringBuffer buffer = new StringBuffer();
			currentPos = 0;
			historyBuffer = null;
			int historyNum = history.size();
			boolean hintedHistory = false;
			while (true) {

				boolean escape = false;
				boolean ctrl = false;
				int next = inStream.read();
				if (next == 27) {
					escape = true;
					inStream.read();
					next = inStream.read();
				}
				if (escape) {
					if (next == 49) {
						inStream.read();
						next = inStream.read();
					}
					if (next == 53) {
						ctrl = true;
						next = inStream.read();
					}
					if (ctrl) {
						if (next == RIGHT_CHAR) {
							currentPos = buffer.indexOf(" ", currentPos) + 1;
							if (currentPos == 0)
								currentPos = buffer.length();
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							rewriteConsole(cleaner, true);
							rewriteConsole(buffer, false);
						} else if (next == LEFT_CHAR) {
							if (currentPos > 1 && currentPos < buffer.length() && buffer.charAt(currentPos - 1) == ' ') {
								currentPos = buffer.lastIndexOf(" ", (currentPos - 2)) + 1;
							} else {
								currentPos = buffer.lastIndexOf(" ", currentPos) + 1;
							}
							if (currentPos < 0)
								currentPos = 0;
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							rewriteConsole(cleaner, true);
							rewriteConsole(buffer, false);
						} else {
						}
					} else {
						if (next == UP_CHAR && !history.isEmpty()) {
							if (history.size() > 0) { // UP
								StringBuffer cleaner = new StringBuffer();
								for (int i = 0; i < buffer.length(); i++) {
									cleaner.append(" ");
								}
								rewriteConsole(cleaner, true);
								if (!hintedHistory && (historyNum == history.size() || !buffer.toString().equals(history.get(historyNum)))) {
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
								currentPos = buffer.length();
								rewriteConsole(buffer, false);
								// writeHistory(historyNum);
							}
						} else if (next == DOWN_CHAR && !history.isEmpty()) { // DOWN
							if (history.size() > 0) {
								StringBuffer cleaner = new StringBuffer();
								for (int i = 0; i < buffer.length(); i++) {
									cleaner.append(" ");
								}
								rewriteConsole(cleaner, true);

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
								currentPos = buffer.length();
								rewriteConsole(buffer, false);
								// writeHistory(historyNum);
							}
						} else if (next == RIGHT_CHAR) {
							if (currentPos < buffer.length()) {
								currentPos++;
								StringBuffer cleaner = new StringBuffer();
								for (int i = 0; i < buffer.length(); i++) {
									cleaner.append(" ");
								}
								rewriteConsole(cleaner, true);
								rewriteConsole(buffer, false);
							}
						} else if (next == LEFT_CHAR) {
							if (currentPos > 0) {
								currentPos--;
								StringBuffer cleaner = new StringBuffer();
								for (int i = 0; i < buffer.length(); i++) {
									cleaner.append(" ");
								}
								rewriteConsole(cleaner, true);
								rewriteConsole(buffer, false);
							}
						} else if (next == END_CHAR) {
							currentPos = buffer.length();
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							rewriteConsole(cleaner, true);
							rewriteConsole(buffer, false);
						} else if (next == BEGIN_CHAR) {
							currentPos = 0;
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							rewriteConsole(cleaner, true);
							rewriteConsole(buffer, false);
						} else {
						}
					}
				} else {
					if (next == NEW_LINE_CHAR) {
						outStream.println();
						break;
					} else if (next == BACKSPACE_CHAR) {
						if (buffer.length() > 0 && currentPos > 0) {
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							buffer.deleteCharAt(currentPos - 1);
							currentPos--;
							rewriteConsole(cleaner, true);
							rewriteConsole(buffer, false);
						}
					} else if (next == DEL_CHAR) {
						if (buffer.length() > 0 && currentPos >= 0 && currentPos < buffer.length()) {
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							buffer.deleteCharAt(currentPos);
							rewriteConsole(cleaner, true);
							rewriteConsole(buffer, false);
						}
					} else if (next == HORIZONTAL_TAB_CHAR) {
						StringBuffer cleaner = new StringBuffer();
						for (int i = 0; i < buffer.length(); i++) {
							cleaner.append(" ");
						}
						buffer = writeHint(buffer);
						rewriteConsole(cleaner, true);
						rewriteConsole(buffer, false);
						currentPos = buffer.length();
					} else {
						if (next > UNIT_SEPARATOR_CHAR && next < BACKSPACE_CHAR) {
							StringBuffer cleaner = new StringBuffer();
							for (int i = 0; i < buffer.length(); i++) {
								cleaner.append(" ");
							}
							if (currentPos == buffer.length()) {
								buffer.append((char) next);
							} else {
								buffer.insert(currentPos, (char) next);
							}
							currentPos++;
							rewriteConsole(cleaner, true);
							rewriteConsole(buffer, false);
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
		} catch (IOException e) {
			return null;
		}
		if (consoleInput.equals("clear")) {
			outStream.flush();
			for (int i = 0; i < 150; i++) {
				outStream.println();
			}
			outStream.print("\r");
			outStream.print("> ");
			return readLine();
		} else {
			return consoleInput;
		}
	}

	private void writeHistory(int historyNum) throws IOException {
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
				for (String historyEntry : history.subList(historyNum - MAX_HISTORY_ENTRIES - 1, historyNum - 1)) {
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

	private void rewriteConsole(StringBuffer buffer, boolean cleaner) {
		outStream.print("\r");
		outStream.print("> ");
		if (currentPos < buffer.length() && buffer.length() > 0 && !cleaner) {
			outStream.print("\033[0m" + buffer.substring(0, currentPos) + "\033[0;30;47m" + buffer.substring(currentPos, currentPos + 1)
					+ "\033[0m" + buffer.substring(currentPos + 1) + "\033[0m");
		} else {
			outStream.print(buffer);
		}
	}

	private void rewriteHintConsole(StringBuffer buffer) {
		outStream.print("\r");
		outStream.print(buffer);
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
		File file = new File(HISTORY_FILE_NAME);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException ioe) {
				OLogManager.instance().error(this, "Error creating history file.", ioe, "");
			}
		} else if (!read) {
			file.delete();
			try {
				file.createNewFile();
			} catch (IOException ioe) {
				OLogManager.instance().error(this, "Error creating history file.", ioe, "");
			}
		}
		return file;
	}

}
