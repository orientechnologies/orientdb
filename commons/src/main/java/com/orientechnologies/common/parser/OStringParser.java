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

	public static String[] getWords(String iRecord, final String ioWordSeparator) {
		iRecord = iRecord.trim();
		char separator = ioWordSeparator.charAt(0);

		ArrayList<String> fields = new ArrayList<String>();
		StringBuilder buffer = new StringBuilder();
		char stringBeginChar = ' ';
		char c;

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
			} else if (c == separator && stringBeginChar == ' ') {
				if (buffer.length() > 0) {
					// SEPARATOR (OUTSIDE A STRING): PUSH
					fields.add(buffer.toString());
					buffer.setLength(0);
				}
				continue;
			}

			buffer.append(c);
		}
		fields.add(buffer.toString());
		buffer.setLength(0);

		String[] result = new String[fields.size()];
		fields.toArray(result);
		return result;
	}

}
