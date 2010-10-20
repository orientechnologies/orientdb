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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;

public abstract class OStringSerializerHelper {
	public static final char								RECORD_SEPARATOR			= ',';

	public static final String							CLASS_SEPARATOR				= "@";
	public static final char								LINK									= '#';
	public static final char								EMBEDDED							= '*';
	public static final String							OPEN_BRACE						= "(";
	public static final String							CLOSED_BRACE					= ")";
	public static final char								COLLECTION_BEGIN			= '[';
	public static final char								COLLECTION_END				= ']';
	public static final char								MAP_BEGIN							= '{';
	public static final char								MAP_END								= '}';
	public static final char								ENTRY_SEPARATOR				= ':';
	public static final char								PARAMETER_SEPARATOR		= ',';
	public static final char								COLLECTION_SEPARATOR	= ',';
	public static final List<String>				EMPTY_LIST						= Collections.unmodifiableList(new ArrayList<String>());
	public static final Map<String, String>	EMPTY_MAP							= Collections.unmodifiableMap(new HashMap<String, String>());

	public static Object fieldTypeFromStream(OType iType, final Object iValue) {
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
			return new Integer(iValue.toString());

		case BOOLEAN:
			if (iValue instanceof Boolean)
				return iValue;
			return new Boolean(iValue.toString());

		case FLOAT:
			if (iValue instanceof Float)
				return iValue;
			return new Float(iValue.toString());

		case LONG:
			if (iValue instanceof Long)
				return iValue;
			return new Long(iValue.toString());

		case DOUBLE:
			if (iValue instanceof Double)
				return iValue;
			return new Double(iValue.toString());

		case SHORT:
			if (iValue instanceof Short)
				return iValue;
			return new Short(iValue.toString());

		case BYTE:
			if (iValue instanceof Byte)
				return iValue;
			return new Byte(iValue.toString());

		case BINARY:
			if (iValue instanceof byte[])
				return iValue;
			try {
				return OBase64Utils.decode((String) iValue);
			} catch (IOException e) {
				throw new OSerializationException("Error on decode binary data", e);
			}

		case DATE:
			if (iValue instanceof Date)
				return iValue;
			return new Date(Long.parseLong(iValue.toString()));

		case LINK:
			if (iValue instanceof ORID)
				return iValue.toString();
			else if (iValue instanceof String)
				return new ORecordId((String) iValue);
			else
				return ((ORecord<?>) iValue).getIdentity().toString();

		case EMBEDDED:
			// RECORD
			return OStringSerializerAnyStreamable.INSTANCE.fromStream((String) iValue);
		}

		throw new IllegalArgumentException("Type " + iType + " not supported to convert value: " + iValue);
	}

	public static String fieldTypeToString(OType iType, final Object iValue) {
		if (iValue == null)
			return null;

		if (iType == null)
			iType = OType.EMBEDDED;

		switch (iType) {
		case STRING:
			return "\"" + encode((String) iValue) + "\"";

		case INTEGER:
		case FLOAT:
		case LONG:
		case DOUBLE:
		case SHORT:
		case BYTE:
		case BOOLEAN:
			return String.valueOf(iValue);

		case BINARY:
			if (iValue instanceof Byte)
				return new String(new byte[] { ((Byte) iValue).byteValue() });
			return OBase64Utils.encodeBytes((byte[]) iValue);

		case DATE:
			return String.valueOf(((Date) iValue).getTime());

		case LINK:
			if (iValue instanceof ORID)
				return iValue.toString();
			else
				return ((ORecord<?>) iValue).getIdentity().toString();

		case EMBEDDED:
			// RECORD
			return OStringSerializerAnyStreamable.INSTANCE.toStream(iValue);
		}

		throw new IllegalArgumentException("Type " + iType + " not supported to convert value: " + iValue);
	}

	public static List<String> smartSplit(final String iSource, final char iRecordSeparator) {
		StringBuilder buffer = new StringBuilder();
		char stringBeginChar = ' ';
		char c;
		char previousChar = ' ';
		boolean insideEmbedded = false;
		int insideCollection = 0;
		int insideMap = 0;
		int insideLinkPart = 0;

		final ArrayList<String> parts = new ArrayList<String>();
		final int max = iSource.length();

		for (int i = 0; i < max; ++i) {
			c = iSource.charAt(i);

			if (stringBeginChar == ' ') {
				// OUTSIDE A STRING

				if (c == COLLECTION_BEGIN)
					insideCollection++;
				else if (c == MAP_BEGIN)
					insideMap++;
				else if (c == COLLECTION_END) {
					if (insideCollection == 0)
						throw new OSerializationException("Found invalid " + COLLECTION_END + " character. Assure to open and close correctly.");
					insideCollection--;
				} else if (c == MAP_END) {
					if (insideMap == 0)
						throw new OSerializationException("Found invalid " + MAP_END + " character. Assure to open and close correctly.");
					insideMap--;
				} else if (c == EMBEDDED)
					insideEmbedded = !insideEmbedded;
				else if (c == LINK)
					// FIRST PART OF LINK
					insideLinkPart = 1;
				else if (insideLinkPart == 1 && c == ORID.SEPARATOR)
					// SECOND PART OF LINK
					insideLinkPart = 2;

				if (insideLinkPart > 0 && c != '-' && !Character.isDigit(c) && c != ORID.SEPARATOR && c != LINK)
					insideLinkPart = 0;

				if (insideCollection == 0 && insideMap == 0 && !insideEmbedded && insideLinkPart == 0) {
					// OUTSIDE A COLLECTION/MAP
					if ((c == '\'' || c == '"') && previousChar != '\\') {
						// START STRING
						stringBeginChar = c;
					} else if (c == iRecordSeparator) {
						// SEPARATOR (OUTSIDE A STRING): PUSH
						parts.add(buffer.toString());
						buffer.setLength(0);
						continue;
					}
				}

			} else {
				// INSIDE A STRING
				if ((c == '\'' || c == '"') && previousChar != '\\') {
					// CLOSE THE STRING ?
					if (stringBeginChar == c) {
						// SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
						stringBeginChar = ' ';
					}
				}
			}

			buffer.append(c);

			previousChar = c;
		}

		parts.add(buffer.toString());

		return parts;
	}

	public static List<String> split(final String iSource, final char iRecordSeparator, final char... iJumpCharacters) {
		final ArrayList<String> parts = new ArrayList<String>();
		final int max = iSource.length();
		final StringBuilder buffer = new StringBuilder();
		char c;

		for (int i = 0; i < max; ++i) {
			c = iSource.charAt(i);

			if (c == iRecordSeparator) {
				parts.add(buffer.toString());
				buffer.setLength(0);
			} else {
				boolean toAppend = true;
				if (iJumpCharacters.length > 0 && buffer.length() == 0) {
					// CHECK IF IT'S A CHAR TO JUMP
					for (char j : iJumpCharacters) {
						if (c == j) {
							// JUMP THE CHAR
							toAppend = false;
							break;
						}
					}
				}
				if (toAppend)
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

		parts.add(buffer.toString());

		return parts;
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

	public static List<String> getCollection(final String iText) {
		int openPos = iText.indexOf(COLLECTION_BEGIN);
		if (openPos == -1)
			return EMPTY_LIST;

		int closePos = iText.indexOf(COLLECTION_END, openPos + 1);
		if (closePos == -1)
			return EMPTY_LIST;

		return split(iText.substring(openPos + 1, closePos), COLLECTION_SEPARATOR, ' ');
	}

	public static List<String> getParameters(final String iText, final int iBeginPosition) {
		int openPos = iText.indexOf(OPEN_BRACE, iBeginPosition);
		if (openPos == -1)
			return EMPTY_LIST;

		int closePos = iText.indexOf(CLOSED_BRACE, openPos + 1);
		if (closePos == -1)
			return EMPTY_LIST;

		if (closePos - openPos == 1)
			// EMPTY STRING: TREATS AS EMPTY
			return EMPTY_LIST;

		final List<String> pars = split(iText.substring(openPos + 1, closePos), PARAMETER_SEPARATOR, ' ');

		// REMOVE TAIL AND END SPACES
		for (int i = 0; i < pars.size(); ++i)
			pars.set(i, pars.get(i));

		return pars;
	}

	public static List<String> getParameters(final String iText) {
		return getParameters(iText, 0);
	}

	public static Map<String, String> getMap(final String iText) {
		int openPos = iText.indexOf(COLLECTION_BEGIN);
		if (openPos == -1)
			return EMPTY_MAP;

		int closePos = iText.indexOf(COLLECTION_END, openPos + 1);
		if (closePos == -1)
			return EMPTY_MAP;

		final List<String> entries = smartSplit(iText.substring(openPos + 1, closePos), COLLECTION_SEPARATOR);
		if (entries.size() == 0)
			return EMPTY_MAP;

		Map<String, String> map = new HashMap<String, String>();

		List<String> entry;
		for (String item : entries) {
			if (item != null && item.length() > 0) {
				entry = OStringSerializerHelper.split(item, OStringSerializerHelper.ENTRY_SEPARATOR);

				map.put((String) fieldTypeFromStream(OType.STRING, entry.get(0)), entry.get(1));
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

		for (int i = 0; i < iText.length(); ++i)
			if (iText.charAt(i) == '"' || iText.charAt(i) == '\\' || iText.charAt(i) == '\'') {
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

			if (c == '"' || c == '\\' || c == '\'') {
				buffer.insert(i, '\\');
				++i;
			}
		}

		return buffer.toString();
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
}
