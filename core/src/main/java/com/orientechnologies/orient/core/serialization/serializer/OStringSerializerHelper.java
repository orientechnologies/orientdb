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
import java.util.Date;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;

public abstract class OStringSerializerHelper {
	public static final String	RECORD_SEPARATOR					= ",";
	public static final char		RECORD_SEPARATOR_AS_CHAR	= ',';

	public static final String	CLASS_SEPARATOR						= "@";
	public static final String	LINK											= "#";
	public static final char		EMBEDDED									= '*';
	public static final char		COLLECTION_BEGIN					= '[';
	public static final char		COLLECTION_END						= ']';
	public static final char		MAP_BEGIN									= '{';
	public static final char		MAP_END										= '}';
	public static final String	ENTRY_SEPARATOR						= ":";

	public static Object fieldTypeFromStream(OType iType, Object iValue) {
		if (iValue == null)
			return null;

		switch (iType) {
		case STRING:
			if (iValue instanceof String) {
				final String s = (String) iValue;
				return s.substring(1, s.length() - 1);
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

		case BINARY:
			if (iValue instanceof byte[])
				return iValue;
			return iValue.toString().getBytes();

		case DATE:
			if (iValue instanceof Date)
				return iValue;
			return new Date(Long.parseLong(iValue.toString()));

		case LINK:
			if (iValue instanceof ORID)
				return iValue.toString();
			else
				return ((ORecord<?>) iValue).getIdentity().toString();

		case EMBEDDED:
			// RECORD
			return OStringSerializerAnyStreamable.INSTANCE.fromStream((String) iValue);
		}

		throw new IllegalArgumentException("Type " + iType + " not supported to convert value: " + iValue);
	}

	public static String fieldTypeToString(OType iType, Object iValue) {
		if (iValue == null)
			return null;

		switch (iType) {
		case STRING:
			return "\"" + (String) iValue + "\"";

		case INTEGER:
		case FLOAT:
		case LONG:
		case DOUBLE:
		case SHORT:
			return String.valueOf(iValue);

		case BINARY:
			if (iValue instanceof Byte)
				return new String(new byte[] { ((Byte) iValue).byteValue() });
			return new String((byte[]) iValue);

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

	public static String[] split(String iSource, char iRecordSeparator) {
		StringBuilder buffer = new StringBuilder();
		char stringBeginChar = ' ';
		char c;
		boolean insideEmbedded = false;
		int insideCollection = 0;
		int insideMap = 0;

		ArrayList<String> parts = new ArrayList<String>();

		for (int i = 0; i < iSource.length(); ++i) {
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

				if (insideCollection == 0 && insideMap == 0 && !insideEmbedded) {
					// OUTSIDE A COLLECTION

					if (c == '\'' || c == '"') {
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
				if (c == '\'' || c == '"') {
					// CLOSE THE STRING ?
					if (stringBeginChar == c) {
						// SAME CHAR AS THE BEGIN OF THE STRING: CLOSE IT AND PUSH
						stringBeginChar = ' ';
					}
				}
			}

			buffer.append(c);
		}

		parts.add(buffer.toString());

		return parts.toArray(new String[parts.size()]);
	}

	public static String joinIntArray(int[] iArray) {
		StringBuilder ids = new StringBuilder();
		for (int id : iArray) {
			if (ids.length() > 0)
				ids.append(RECORD_SEPARATOR_AS_CHAR);
			ids.append(id);
		}
		return ids.toString();
	}

	public static int[] splitIntArray(String iInput) {
		String[] items = iInput.split(RECORD_SEPARATOR);
		int[] values = new int[items.length];
		for (int i = 0; i < items.length; ++i) {
			values[i] = Integer.parseInt(items[i]);
		}
		return values;
	}
}
