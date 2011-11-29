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
package com.orientechnologies.common.parser;

import java.util.ArrayList;

/**
 * String parser utility class
 * 
 * @author Luca Garulli
 * 
 */
public class OStringParser {

	public static final String	WHITE_SPACE	= " ";
	public static final String	COMMON_JUMP	= " \r\n";

	public static String getWordFromString(String iText, final int iBeginIndex, final String ioSeparatorChars) {
		return getWord(iText.trim(), iBeginIndex, ioSeparatorChars);
	}

	public static String getWord(final CharSequence iText, final int iBeginIndex, final String ioSeparatorChars) {
		final StringBuilder buffer = new StringBuilder();
		char stringBeginChar = ' ';
		char c;

		for (int i = iBeginIndex; i < iText.length(); ++i) {
			c = iText.charAt(i);

			if (c == '\'' || c == '"') {
				if (stringBeginChar != ' ') {
					// CLOSE THE STRING?
					if (stringBeginChar == c) {
						// SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
						stringBeginChar = ' ';
					}
				} else {
					// START STRING
					stringBeginChar = c;
				}
			} else if (stringBeginChar == ' ') {
				for (int sepIndex = 0; sepIndex < ioSeparatorChars.length(); ++sepIndex) {
					if (ioSeparatorChars.charAt(sepIndex) == c && buffer.length() > 0)
						// SEPARATOR (OUTSIDE A STRING): PUSH
						return buffer.toString();
				}
			}

			buffer.append(c);
		}

		return buffer.toString();
	}

	public static String[] getWords(String iRecord, final String iSeparatorChars) {
		return getWords(iRecord, iSeparatorChars, false);
	}

	public static String[] getWords(String iRecord, final String iSeparatorChars, final boolean iIncludeStringSep) {
		return getWords(iRecord, iSeparatorChars, " \n\r\t", iIncludeStringSep);
	}

	public static String[] getWords(String iText, final String iSeparatorChars, final String iJumpChars,
			final boolean iIncludeStringSep) {
		iText = iText.trim();

		final ArrayList<String> fields = new ArrayList<String>();
		final StringBuilder buffer = new StringBuilder();
		char stringBeginChar = ' ';
		char c;
		int openBraket = 0;
		int openGraph = 0;
		boolean charFound;
		boolean escape = false;

		for (int i = 0; i < iText.length(); ++i) {
			c = iText.charAt(i);

			if (openBraket == 0 && openGraph == 0 && !escape && c == '\\') {
				// ESCAPE CHARS
				final char nextChar = iText.charAt(i + 1);

				if (nextChar == 'u') {
					i = readUnicode(iText, i + 2, buffer);
				} else if (nextChar == 'n') {
					buffer.append("\n");
					i++;
				} else if (nextChar == 'r') {
					buffer.append("\r");
					i++;
				} else if (nextChar == 't') {
					buffer.append("\t");
					i++;
				} else if (nextChar == 'f') {
					buffer.append("\f");
					i++;
				} else
					escape = true;

				continue;
			}

			if (openBraket == 0 && openGraph == 0 && !escape && (c == '\'' || c == '"')) {
				if (stringBeginChar != ' ') {
					// CLOSE THE STRING?
					if (stringBeginChar == c) {
						// SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
						stringBeginChar = ' ';

						if (iIncludeStringSep)
							buffer.append(c);

						fields.add(buffer.toString());
						buffer.setLength(0);
						continue;
					}
				} else {
					// START STRING
					stringBeginChar = c;
					if (iIncludeStringSep)
						buffer.append(c);

					continue;
				}
			} else if (stringBeginChar == ' ') {
				if (c == '[')
					openBraket++;
				else if (c == ']')
					openBraket--;
				if (c == '{')
					openGraph++;
				else if (c == '}')
					openGraph--;
				else if (openBraket == 0 && openGraph == 0) {
					charFound = false;
					for (int sepIndex = 0; sepIndex < iSeparatorChars.length(); ++sepIndex) {
						if (iSeparatorChars.charAt(sepIndex) == c) {
							charFound = true;
							if (buffer.length() > 0) {
								// SEPARATOR (OUTSIDE A STRING): PUSH
								fields.add(buffer.toString());
								buffer.setLength(0);
							}
							break;
						}
					}

					if (charFound)
						continue;
				}

				if (stringBeginChar == ' ') {
					// CHECK FOR CHAR TO JUMP
					charFound = false;

					for (int jumpIndex = 0; jumpIndex < iJumpChars.length(); ++jumpIndex) {
						if (iJumpChars.charAt(jumpIndex) == c) {
							charFound = true;
							break;
						}
					}

					if (charFound)
						continue;
				}
			}

			buffer.append(c);

			if (escape)
				escape = false;
		}

		if (buffer.length() > 0)
			// ADD THE LAST WORD IF ANY
			fields.add(buffer.toString());

		String[] result = new String[fields.size()];
		fields.toArray(result);
		return result;
	}

	public static String[] split(String iText, final char iSplitChar, String iJumpChars) {
		iText = iText.trim();

		ArrayList<String> fields = new ArrayList<String>();
		StringBuilder buffer = new StringBuilder();
		char c;
		char stringChar = ' ';
		boolean escape = false;
		boolean jumpSplitChar = false;
		boolean charFound;

		for (int i = 0; i < iText.length(); i++) {
			c = iText.charAt(i);

			if (!escape && c == '\\') {
				if (iText.charAt(i + 1) == 'u') {
					i = readUnicode(iText, i + 2, buffer);
				} else {
					escape = true;
					buffer.append(c);
				}
				continue;
			}

			if (c == '\'' || c == '"') {
				if (!jumpSplitChar) {
					jumpSplitChar = true;
					stringChar = c;
				} else {
					if (!escape && c == stringChar)
						jumpSplitChar = false;
				}
			}

			if (c == iSplitChar) {
				if (!jumpSplitChar) {
					fields.add(buffer.toString());
					buffer.setLength(0);
					continue;
				}
			}

			// CHECK IF IT MUST JUMP THE CHAR
			if (buffer.length() == 0) {
				charFound = false;

				for (int jumpIndex = 0; jumpIndex < iJumpChars.length(); ++jumpIndex) {
					if (iJumpChars.charAt(jumpIndex) == c) {
						charFound = true;
						break;
					}
				}

				if (charFound)
					continue;
			}

			buffer.append(c);

			if (escape)
				escape = false;
		}

		if (buffer.length() > 0) {
			fields.add(buffer.toString());
			buffer.setLength(0);
		}
		String[] result = new String[fields.size()];
		fields.toArray(result);
		return result;
	}

	/**
	 * Jump white spaces.
	 * 
	 * @param iText
	 *          String to analyze
	 * @param iCurrentPosition
	 *          Current position in text
	 * @return The new offset inside the string analyzed
	 */
	public static int jumpWhiteSpaces(final CharSequence iText, final int iCurrentPosition) {
		return jump(iText, iCurrentPosition, WHITE_SPACE);
	}

	/**
	 * Jump some characters reading from an offset of a String.
	 * 
	 * @param iText
	 *          String to analyze
	 * @param iCurrentPosition
	 *          Current position in text
	 * @param iJumpChars
	 *          String as char array of chars to jump
	 * @return The new offset inside the string analyzed
	 */
	public static int jump(final CharSequence iText, int iCurrentPosition, final String iJumpChars) {
		if (iCurrentPosition < 0)
			return -1;

		final int size = iText.length();
		final int jumpCharSize = iJumpChars.length();
		boolean found = true;
		char c;
		for (; iCurrentPosition < size; ++iCurrentPosition) {
			found = false;
			c = iText.charAt(iCurrentPosition);
			for (int jumpIndex = 0; jumpIndex < jumpCharSize; ++jumpIndex) {
				if (iJumpChars.charAt(jumpIndex) == c) {
					found = true;
					break;
				}
			}

			if (!found)
				break;
		}

		return iCurrentPosition >= size ? -1 : iCurrentPosition;
	}

	public static int readUnicode(String iText, int position, StringBuilder buffer) {
		// DECODE UNICODE CHAR
		final StringBuilder buff = new StringBuilder();
		final int lastPos = position + 4;
		for (; position < lastPos; ++position)
			buff.append(iText.charAt(position));

		buffer.append((char) Integer.parseInt(buff.toString(), 16));
		return position - 1;
	}
}
