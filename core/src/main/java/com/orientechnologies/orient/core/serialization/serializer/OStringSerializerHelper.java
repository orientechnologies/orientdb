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
package com.orientechnologies.orient.core.serialization.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;

public abstract class OStringSerializerHelper {
	public static final char								RECORD_SEPARATOR				= ',';

	public static final String							CLASS_SEPARATOR					= "@";
	public static final char								LINK										= ORID.PREFIX;
	public static final char								EMBEDDED_BEGIN					= '(';
	public static final char								EMBEDDED_END						= ')';
	public static final char								COLLECTION_BEGIN				= '[';
	public static final char								COLLECTION_END					= ']';
	public static final char								MAP_BEGIN								= '{';
	public static final char								MAP_END									= '}';
	public static final char								BINARY_BEGINEND					= '_';
	public static final char								CUSTOM_TYPE							= '^';
	public static final char								ENTRY_SEPARATOR					= ':';
	public static final char								PARAMETER_NAMED					= ':';
	public static final char								PARAMETER_POSITIONAL		= '?';
	public static final char[]							PARAMETER_SEPARATOR			= new char[] { ',', ')' };
	public static final char[]							PARAMETER_EXT_SEPARATOR	= new char[] { ' ', '.' };
	public static final char								COLLECTION_SEPARATOR		= ',';
	public static final List<String>				EMPTY_LIST							= Collections.unmodifiableList(new ArrayList<String>());
	public static final Map<String, String>	EMPTY_MAP								= Collections.unmodifiableMap(new HashMap<String, String>());

	public static Object fieldTypeFromStream(final ODocument iDocument, OType iType, final Object iValue) {
		if (iValue == null)
			return null;

		if (iType == null)
			iType = OType.EMBEDDED;

		switch (iType) {
		case STRING:
			if (iValue instanceof String) {
				final String s = (String) iValue;
				return decode(s.substring(1, s.length() - 1));
			}
			return iValue.toString();

		case INTEGER:
			if (iValue instanceof Integer)
				return iValue;
			return new Integer(getStringContent(iValue));

		case BOOLEAN:
			if (iValue instanceof Boolean)
				return iValue;
			return new Boolean(getStringContent(iValue));

		case FLOAT:
			if (iValue instanceof Float)
				return iValue;
			return new Float(getStringContent(iValue));

		case LONG:
			if (iValue instanceof Long)
				return iValue;
			return new Long(getStringContent(iValue));

		case DOUBLE:
			if (iValue instanceof Double)
				return iValue;
			return new Double(getStringContent(iValue));

		case SHORT:
			if (iValue instanceof Short)
				return iValue;
			return new Short(getStringContent(iValue));

		case BYTE:
			if (iValue instanceof Byte)
				return iValue;
			return new Byte(getStringContent(iValue));

		case BINARY:
			return getBinaryContent(iValue);

		case DATE:
		case DATETIME:
			if (iValue instanceof Date)
				return iValue;
			return new Date(Long.parseLong(getStringContent(iValue)));

		case LINK:
			if (iValue instanceof ORID)
				return iValue.toString();
			else if (iValue instanceof String)
				return new ORecordId((String) iValue);
			else
				return ((ORecord<?>) iValue).getIdentity().toString();

		case EMBEDDED:
			// EMBEDDED
			return OStringSerializerAnyStreamable.INSTANCE.fromStream((String) iValue);

		case EMBEDDEDMAP:
			// RECORD
			final String value = (String) iValue;
			return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapFromStream(iDocument, null, value);
		}

		throw new IllegalArgumentException("Type " + iType + " does not support converting value: " + iValue);
	}

	public static List<String> smartSplit(final String iSource, final char iRecordSeparator, final char... iJumpChars) {
		return smartSplit(iSource, new char[] { iRecordSeparator }, 0, -1, false, iJumpChars);
	}

	public static List<String> smartSplit(final String iSource, final char[] iRecordSeparator, int beginIndex, final int endIndex,
			final boolean iStringSeparatorExtended, final char... iJumpChars) {
		final StringBuilder buffer = new StringBuilder();
		final ArrayList<String> parts = new ArrayList<String>();

		while ((beginIndex = parse(iSource, buffer, beginIndex, endIndex, iRecordSeparator, iStringSeparatorExtended, iJumpChars)) > -1) {
			parts.add(buffer.toString());
			buffer.setLength(0);
		}

		if (buffer.length() > 0)
			parts.add(buffer.toString());

		return parts;
	}

	public static int parse(final String iSource, final StringBuilder iBuffer, final int beginIndex, final int endIndex,
			final char[] iRecordSeparator, final boolean iStringSeparatorExtended, final char... iJumpChars) {
		char stringBeginChar = ' ';
		boolean encodeMode = false;
		int insideParenthesis = 0;
		int insideCollection = 0;
		int insideMap = 0;
		int insideLinkPart = 0;

		final int max = endIndex > -1 ? endIndex + 1 : iSource.length();

		final char[] buffer = new char[max - beginIndex];
		iSource.getChars(beginIndex, max, buffer, 0);

		for (int i = 0; i < buffer.length; ++i) {
			final char c = buffer[i];

			if (stringBeginChar == ' ') {
				// OUTSIDE A STRING

				if (c == COLLECTION_BEGIN)
					insideCollection++;
				else if (c == COLLECTION_END) {
					if (!isCharPresent(c, iRecordSeparator)) {
						if (insideCollection == 0)
							throw new OSerializationException("Found invalid " + COLLECTION_END
									+ " character. Ensure it is opened and closed correctly.");
						insideCollection--;
					}

				} else if (c == EMBEDDED_BEGIN) {
					insideParenthesis++;
				} else if (c == EMBEDDED_END) {
					if (!isCharPresent(c, iRecordSeparator)) {
						if (insideParenthesis == 0)
							throw new OSerializationException("Found invalid " + EMBEDDED_END
									+ " character. Ensure it is opened and closed correctly.");
						insideParenthesis--;
					}

				} else if (c == MAP_BEGIN) {
					insideMap++;
				} else if (c == MAP_END) {
					if (!isCharPresent(c, iRecordSeparator)) {
						if (insideMap == 0)
							throw new OSerializationException("Found invalid " + MAP_END
									+ " character. Ensure it is opened and closed correctly.");
						insideMap--;
					}
				}

				else if (c == LINK)
					// FIRST PART OF LINK
					insideLinkPart = 1;
				else if (insideLinkPart == 1 && c == ORID.SEPARATOR)
					// SECOND PART OF LINK
					insideLinkPart = 2;

				if (insideLinkPart > 0 && c != '-' && !Character.isDigit(c) && c != ORID.SEPARATOR && c != LINK)
					insideLinkPart = 0;

				if ((c == '"' || iStringSeparatorExtended && c == '\'') && !encodeMode) {
					// START STRING
					stringBeginChar = c;
				}

				if (insideParenthesis == 0 && insideCollection == 0 && insideMap == 0 && insideLinkPart == 0) {
					// OUTSIDE A PARAMS/COLLECTION/MAP
					if (isCharPresent(c, iRecordSeparator)) {
						// SEPARATOR (OUTSIDE A STRING): PUSH
						return beginIndex + i + 1;
					}
				}

				if (iJumpChars.length > 0) {
					if (isCharPresent(c, iJumpChars))
						continue;
				}

			} else {
				// INSIDE A STRING
				if ((c == '"' || iStringSeparatorExtended && c == '\'') && !encodeMode) {
					// CLOSE THE STRING ?
					if (stringBeginChar == c) {
						// SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
						stringBeginChar = ' ';
					}
				}
			}

			if (c == '\\' && !encodeMode) {
				// ESCAPE CHARS
				final char nextChar = buffer[i + 1];
				if (nextChar == 'u') {
					i = OStringParser.readUnicode(buffer, i + 2, iBuffer);
					continue;
				} else if (nextChar == 'n') {
					iBuffer.append("\n");
					i++;
					continue;
				} else if (nextChar == 'r') {
					iBuffer.append("\r");
					i++;
					continue;
				} else if (nextChar == 't') {
					iBuffer.append("\t");
					i++;
					continue;
				} else if (nextChar == 'f') {
					iBuffer.append("\f");
					i++;
					continue;
				} else
					encodeMode = true;
			} else
				encodeMode = false;

			if (c != '\\' && encodeMode) {
				encodeMode = false;
			}

			iBuffer.append(c);
		}
		return -1;
	}

	public static boolean isCharPresent(final char iCharacter, final char[] iCharacters) {
		final int len = iCharacters.length;
		for (int i = 0; i < len; ++i) {
			if (iCharacter == iCharacters[i]) {
				return true;
			}
		}

		return false;
	}

	public static List<String> split(final String iSource, final char iRecordSeparator, final char... iJumpCharacters) {
		return split(iSource, 0, iSource.length(), iRecordSeparator, iJumpCharacters);
	}

	public static Collection<String> split(final Collection<String> iParts, final String iSource, final char iRecordSeparator,
			final char... iJumpCharacters) {
		return split(iParts, iSource, 0, iSource.length(), iRecordSeparator, iJumpCharacters);
	}

	public static List<String> split(final String iSource, final int iStartPosition, final int iEndPosition,
			final char iRecordSeparator, final char... iJumpCharacters) {
		return (List<String>) split(new ArrayList<String>(), iSource, 0, iSource.length(), iRecordSeparator, iJumpCharacters);
	}

	public static Collection<String> split(final Collection<String> iParts, final String iSource, final int iStartPosition,
			final int iEndPosition, final char iRecordSeparator, final char... iJumpCharacters) {
		final StringBuilder buffer = new StringBuilder();

		for (int i = iStartPosition; i < iEndPosition; ++i) {
			char c = iSource.charAt(i);

			if (c == iRecordSeparator) {
				iParts.add(buffer.toString());
				buffer.setLength(0);
			} else {
				if (iJumpCharacters.length > 0 && buffer.length() == 0) {
					// CHECK IF IT'S A CHAR TO JUMP
					if (!isCharPresent(c, iJumpCharacters)) {
						buffer.append(c);
					}
				} else
					buffer.append(c);
			}
		}

		if (iJumpCharacters.length > 0 && buffer.length() > 0) {
			// CHECK THE END OF LAST ITEM IF NEED TO CUT THE CHARS TO JUMP
			char b;
			int newSize = 0;
			boolean found;
			for (int i = buffer.length() - 1; i >= 0; --i) {
				b = buffer.charAt(i);
				found = false;
				for (char j : iJumpCharacters) {
					if (j == b) {
						found = true;
						++newSize;
						break;
					}
				}
				if (!found)
					break;
			}
			if (newSize > 0)
				buffer.setLength(buffer.length() - newSize);
		}

		iParts.add(buffer.toString());

		return iParts;
	}

	public static String joinIntArray(int[] iArray) {
		final StringBuilder ids = new StringBuilder();
		for (int id : iArray) {
			if (ids.length() > 0)
				ids.append(RECORD_SEPARATOR);
			ids.append(id);
		}
		return ids.toString();
	}

	public static int[] splitIntArray(final String iInput) {
		final List<String> items = split(iInput, RECORD_SEPARATOR);
		final int[] values = new int[items.size()];
		for (int i = 0; i < items.size(); ++i) {
			values[i] = Integer.parseInt(items.get(i));
		}
		return values;
	}

	public static boolean contains(final String iText, final char iSeparator) {
		if (iText == null)
			return false;

		final int max = iText.length();
		for (int i = 0; i < max; ++i) {
			if (iText.charAt(i) == iSeparator)
				return true;
		}

		return false;
	}

	public static int getCollection(final String iText, final int iStartPosition, final Collection<String> iCollection) {
		final StringBuilder buffer = new StringBuilder();

		int openPos = iText.indexOf(COLLECTION_BEGIN, iStartPosition);
		if (openPos == -1)
			return -1;

		int currentPos, deep;
		for (currentPos = openPos + 1, deep = 1; deep > 0; currentPos++) {
			if (currentPos >= iText.length()) {
				return -1;
			}

			char c = iText.charAt(currentPos);

			if (buffer.length() == 0 && c == ' ') {
				continue;
			}

			switch (c) {
			case COLLECTION_BEGIN:
				buffer.append(c);
				deep++;
				break;

			case COLLECTION_END:
				if (deep > 1)
					buffer.append(c);
				deep--;
				break;

			case COLLECTION_SEPARATOR:
				if (deep > 1) {
					buffer.append(c);
				} else {
					iCollection.add(buffer.toString().trim());
					buffer.setLength(0);
				}
				break;

			default:
				buffer.append(c);
			}
		}

		iCollection.add(buffer.toString().trim());

		return --currentPos;
	}

	public static int getParameters(final String iText, final int iBeginPosition, int iEndPosition, final List<String> iParameters) {
		iParameters.clear();

		final int openPos = iText.indexOf(EMBEDDED_BEGIN, iBeginPosition);
		if (openPos == -1 || (iEndPosition > -1 && openPos > iEndPosition))
			return iBeginPosition;

		final StringBuilder buffer = new StringBuilder();
		parse(iText, buffer, openPos, iEndPosition, PARAMETER_EXT_SEPARATOR, true);
		if (buffer.length() == 0)
			return iBeginPosition;

		final String t = buffer.substring(1, buffer.length() - 1);
		final List<String> pars = smartSplit(t, PARAMETER_SEPARATOR, 0, -1, true);

		for (int i = 0; i < pars.size(); ++i)
			iParameters.add(pars.get(i).trim());

		return iBeginPosition + buffer.length();
	}

	public static List<String> getParameters(final String iText) {
		final List<String> params = new ArrayList<String>();
		getParameters(iText, 0, -1, params);
		return params;
	}

	public static Map<String, String> getMap(final String iText) {
		int openPos = iText.indexOf(MAP_BEGIN);
		if (openPos == -1)
			return EMPTY_MAP;

		int closePos = iText.indexOf(MAP_END, openPos + 1);
		if (closePos == -1)
			return EMPTY_MAP;

		final List<String> entries = smartSplit(iText.substring(openPos + 1, closePos), COLLECTION_SEPARATOR);
		if (entries.size() == 0)
			return EMPTY_MAP;

		Map<String, String> map = new HashMap<String, String>();

		List<String> entry;
		for (String item : entries) {
			if (item != null && !item.isEmpty()) {
				entry = OStringSerializerHelper.split(item, OStringSerializerHelper.ENTRY_SEPARATOR);

				map.put((String) fieldTypeFromStream(null, OType.STRING, entry.get(0)), entry.get(1));
			}
		}

		return map;
	}

	/**
	 * Transforms, only if needed, the source string escaping the characters \ and ".
	 * 
	 * @param iText
	 *          Input String
	 * @return Modified string if needed, otherwise the same input object
	 * @see OStringSerializerHelper#decode(String)
	 */
	public static String encode(final String iText) {
		int pos = -1;

		final int newSize = iText.length();
		for (int i = 0; i < newSize; ++i) {
			final char c = iText.charAt(i);

			if (c == '"' || c == '\\') {
				pos = i;
				break;
			}
		}

		if (pos > -1) {
			// CHANGE THE INPUT STRING
			final StringBuilder iOutput = new StringBuilder();

			char c;
			for (int i = 0; i < iText.length(); ++i) {
				c = iText.charAt(i);

				if (c == '"' || c == '\\')
					iOutput.append('\\');

				iOutput.append(c);
			}
			return iOutput.toString();
		}

		return iText;
	}

	/**
	 * Transforms, only if needed, the source string un-escaping the characters \ and ".
	 * 
	 * @param iText
	 *          Input String
	 * @return Modified string if needed, otherwise the same input object
	 * @see OStringSerializerHelper#encode(String)
	 */
	public static String decode(final String iText) {
		int pos = -1;

		for (int i = 0; i < iText.length(); ++i)
			if (iText.charAt(i) == '"' || iText.charAt(i) == '\\') {
				pos = i;
				break;
			}

		if (pos == -1)
			// NOT FOUND, RETURN THE SAME STRING (AVOID COPIES)
			return iText;

		// CHANGE THE INPUT STRING
		final StringBuilder buffer = new StringBuilder(iText);

		char c;
		for (int i = pos; i < buffer.length(); ++i) {
			c = buffer.charAt(i);

			if (c == '\\') {
				buffer.deleteCharAt(i);
			}
		}

		return buffer.toString();
	}

	public static OClass getRecordClassName(String iValue, OClass iLinkedClass) {
		// EXTRACT THE CLASS NAME
		int classSeparatorPos = iValue.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
		if (classSeparatorPos > -1) {
			final String className = iValue.substring(0, classSeparatorPos);
			final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();
			if (className != null && database != null)
				iLinkedClass = database.getMetadata().getSchema().getClass(className);

			iValue = iValue.substring(classSeparatorPos + 1);
		}
		return iLinkedClass;
	}

	public static String java2unicode(final String iInput) {
		final StringBuffer result = new StringBuffer();

		char ch;
		String hex;
		for (int i = 0; i < iInput.length(); i++) {
			ch = iInput.charAt(i);

			if (ch >= 0x0020 && ch <= 0x007e) // Does the char need to be converted to unicode?
				result.append(ch); // No.
			else // Yes.
			{
				result.append("\\u"); // standard unicode format.
				hex = Integer.toHexString(iInput.charAt(i) & 0xFFFF); // Get hex value of the char.
				for (int j = 0; j < 4 - hex.length(); j++)
					// Prepend zeros because unicode requires 4 digits
					result.append('0');
				result.append(hex.toLowerCase()); // standard unicode format.
				// ostr.append(hex.toLowerCase(Locale.ENGLISH));
			}
		}

		return result.toString();
	}

	public static String getStringContent(final Object iValue) {
		if (iValue == null)
			return null;

		final String s = iValue.toString();

		if (s == null)
			return null;

		if (s.length() > 1
				&& (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'' || s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'))
			return s.substring(1, s.length() - 1);

		return s;
	}

	/**
	 * Returns the binary representation of a content. If it's a String a Base64 decoding is applied.
	 */
	public static byte[] getBinaryContent(final Object iValue) {
		if (iValue == null)
			return null;
		else if (iValue instanceof byte[])
			return (byte[]) iValue;
		else if (iValue instanceof String) {
			String s = (String) iValue;
			if (s.length() > 1 && (s.charAt(0) == BINARY_BEGINEND && s.charAt(s.length() - 1) == BINARY_BEGINEND)
					|| (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\''))
				// @COMPATIBILITY 1.0rc7-SNAPSHOT ' TO SUPPORT OLD DATABASES
				s = s.substring(1, s.length() - 1);
			else
				throw new IllegalArgumentException("Not binary type: " + iValue);

			return OBase64Utils.decode(s);
		} else
			throw new IllegalArgumentException("Cannot parse binary as the same type as the value (class=" + iValue.getClass().getName()
					+ "): " + iValue);
	}

	/**
	 * Checks if a string contains alphanumeric only characters.
	 * 
	 * @param iContent
	 *          String to check
	 * @return true is all the content is alphanumeric, otherwise false
	 */
	public static boolean isAlphanumeric(final String iContent) {
		final int tot = iContent.length();
		for (int i = 0; i < tot; ++i) {
			if (!Character.isLetterOrDigit(iContent.charAt(i)))
				return false;
		}
		return true;
	}

	public static String removeQuotationMarks(final String iValue) {
		if (iValue != null
				&& iValue.length() > 1
				&& (iValue.charAt(0) == '\'' && iValue.charAt(iValue.length() - 1) == '\'' || iValue.charAt(0) == '"'
						&& iValue.charAt(iValue.length() - 1) == '"'))
			return iValue.substring(1, iValue.length() - 1);

		return iValue;
	}

	public static boolean startsWithIgnoreCase(final String iFirst, final String iSecond) {
		if (iFirst == null)
			throw new IllegalArgumentException("Origin string to compare is null");
		if (iSecond == null)
			throw new IllegalArgumentException("String to match is null");

		final int iSecondLength = iSecond.length();

		if (iSecondLength > iFirst.length())
			return false;

		for (int i = 0; i < iSecondLength; ++i) {
			if (Character.toUpperCase(iFirst.charAt(i)) != Character.toUpperCase(iSecond.charAt(i)))
				return false;
		}
		return true;
	}
}
