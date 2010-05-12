package com.orientechnologies.common.parser;

import java.util.ArrayList;

public class OStringParser {

	public static String getWord(String iText, final int iBeginIndex, final String ioSeparatorChars) {
		iText = iText.trim();
		StringBuilder buffer = new StringBuilder();
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
						buffer.insert(0, "'");
						buffer.append("'");
						return buffer.toString();
					}
				} else {
					// START STRING
					stringBeginChar = c;
					continue;
				}
			} else if (stringBeginChar == ' ') {
				for (int sepIndex = 0; sepIndex < ioSeparatorChars.length(); ++sepIndex) {
					if (ioSeparatorChars.charAt(sepIndex) == c) {
						if (buffer.length() > 0)
							// SEPARATOR (OUTSIDE A STRING): PUSH
							return buffer.toString();
						continue;
					}
				}
			}

			buffer.append(c);
		}

		return buffer.toString();
	}

	public static String[] getWords(String iRecord, final String iSeparatorChars) {
		return getWords(iRecord, iSeparatorChars, " \n\r");
	}

	public static String[] getWords(String iRecord, final String iSeparatorChars, final String iJumpChars) {
		iRecord = iRecord.trim();

		ArrayList<String> fields = new ArrayList<String>();
		StringBuilder buffer = new StringBuilder();
		char stringBeginChar = ' ';
		char c;
		boolean charFound;

		for (int i = 0; i < iRecord.length(); ++i) {
			c = iRecord.charAt(i);
			if (c == '\'' || c == '"') {
				if (stringBeginChar != ' ') {
					// CLOSE THE STRING?
					if (stringBeginChar == c) {
						// SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
						stringBeginChar = ' ';
						fields.add(buffer.toString());
						buffer.setLength(0);
						continue;
					}
				} else {
					// START STRING
					stringBeginChar = c;
					continue;
				}
			} else if (stringBeginChar == ' ') {
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
		}

		if (buffer.length() > 0)
			// ADD THE LAST WORD IF ANY
			fields.add(buffer.toString());

		String[] result = new String[fields.size()];
		fields.toArray(result);
		return result;
	}
}
