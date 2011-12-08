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
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;

@SuppressWarnings("serial")
public abstract class ORecordSerializerStringAbstract implements ORecordSerializer, Serializable {
	private static final char		DECIMAL_SEPARATOR			= '.';
	private static final String	MAX_INTEGER_AS_STRING	= String.valueOf(Integer.MAX_VALUE);
	private static final int		MAX_INTEGER_DIGITS		= MAX_INTEGER_AS_STRING.length();

	protected abstract StringBuilder toString(final ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat,
			final OUserObject2RecordHandler iObjHandler, final Set<Integer> iMarshalledRecords, boolean iOnlyDelta);

	protected abstract ORecordInternal<?> fromString(final ODatabaseRecord iDatabase, final String iContent,
			final ORecordInternal<?> iRecord);

	public StringBuilder toString(final ORecordInternal<?> iRecord, final String iFormat) {
		return toString(iRecord, new StringBuilder(), iFormat, iRecord.getDatabase(), OSerializationThreadLocal.INSTANCE.get(), false);
	}

	public StringBuilder toString(final ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat) {
		return toString(iRecord, iOutput, iFormat, iRecord.getDatabase(), OSerializationThreadLocal.INSTANCE.get(), false);
	}

	public ORecordInternal<?> fromString(final ODatabaseRecord iDatabase, final String iSource) {
		return fromString(iDatabase, iSource, (ORecordInternal<?>) iDatabase.newInstance());
	}

	public ORecordInternal<?> fromStream(final ODatabaseRecord iDatabase, final byte[] iSource, final ORecordInternal<?> iRecord) {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			return fromString(iDatabase, OBinaryProtocol.bytes2string(iSource), iRecord);
		} finally {

			OProfiler.getInstance().stopChrono("ORecordSerializerStringAbstract.fromStream", timer);
		}
	}

	public byte[] toStream(final ODatabaseRecord iDatabase, final ORecordInternal<?> iRecord, boolean iOnlyDelta) {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			return OBinaryProtocol.string2bytes(toString(iRecord, new StringBuilder(), null, iDatabase,
					OSerializationThreadLocal.INSTANCE.get(), iOnlyDelta).toString());
		} finally {

			OProfiler.getInstance().stopChrono("ORecordSerializerStringAbstract.toStream", timer);
		}
	}

	public static Object fieldTypeFromStream(final ODocument iDocument, OType iType, final Object iValue) {
		if (iValue == null)
			return null;

		if (iType == null)
			iType = OType.EMBEDDED;

		switch (iType) {
		case STRING:
			if (iValue instanceof String) {
				final String s = OStringSerializerHelper.getStringContent(iValue);
				return OStringSerializerHelper.decode(s);
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
			return convertValue((String) iValue, iType);

		case LONG:
			if (iValue instanceof Long)
				return iValue;
			return convertValue((String) iValue, iType);

		case DOUBLE:
			if (iValue instanceof Double)
				return iValue;
			return convertValue((String) iValue, iType);

		case SHORT:
			if (iValue instanceof Short)
				return iValue;
			return convertValue((String) iValue, iType);

		case BYTE:
			if (iValue instanceof Byte)
				return iValue;
			return convertValue((String) iValue, iType);

		case BINARY:
			return OStringSerializerHelper.getBinaryContent(iValue);

		case DATE:
		case DATETIME:
			if (iValue instanceof Date)
				return iValue;
			return convertValue((String) iValue, iType);

		case LINK:
			if (iValue instanceof ORID)
				return iValue.toString();
			else if (iValue instanceof String)
				return new ORecordId((String) iValue);
			else
				return ((ORecord<?>) iValue).getIdentity().toString();

		case EMBEDDED:
		case CUSTOM:
			// RECORD
			final Object result = OStringSerializerAnyStreamable.INSTANCE.fromStream(iDocument.getDatabase(), (String) iValue);
			if (result instanceof ODocument)
				((ODocument) result).addOwner(iDocument);
			return result;

		case EMBEDDEDSET:
		case EMBEDDEDLIST: {
			final String value = (String) iValue;
			return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedCollectionFromStream(iDocument.getDatabase(), iDocument, iType,
					null, null, value);
		}

		case EMBEDDEDMAP: {
			final String value = (String) iValue;
			return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapFromStream(iDocument, null, value);
		}
		}

		throw new IllegalArgumentException("Type " + iType + " not supported to convert value: " + iValue);
	}

	public static Object convertValue(final String iValue, final OType iExpectedType) {
		final Object v = getTypeValue((String) iValue);
		return OType.convert(v, iExpectedType.getDefaultJavaType());
	}

	public static void fieldTypeToString(final StringBuilder iBuffer, final ODatabaseComplex<?> iDatabase, OType iType,
			final Object iValue) {
		if (iValue == null)
			return;

		final long timer = OProfiler.getInstance().startChrono();

		if (iType == null) {
			if (iValue instanceof ORID)
				iType = OType.LINK;
			else
				iType = OType.EMBEDDED;
		}

		switch (iType) {
		case STRING:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.string2string", timer);
			break;

		case BOOLEAN:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.bool2string", timer);
			break;

		case INTEGER:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.int2string", timer);
			break;

		case FLOAT:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.float2string", timer);
			break;

		case LONG:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.long2string", timer);
			break;

		case DOUBLE:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.double2string", timer);
			break;

		case SHORT:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.short2string", timer);
			break;

		case BYTE:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.byte2string", timer);
			break;

		case BINARY:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.binary2string", timer);
			break;

		case DATE:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.date2string", timer);
			break;

		case DATETIME:
			simpleValueToStream(iBuffer, iType, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.datetime2string", timer);
			break;

		case LINK:
			if (iValue instanceof ORecordId)
				((ORecordId) iValue).toString(iBuffer);
			else
				((ORecord<?>) iValue).getIdentity().toString(iBuffer);
			OProfiler.getInstance().stopChrono("serializer.rec.str.link2string", timer);
			break;

		case EMBEDDEDSET:
			ORecordSerializerSchemaAware2CSV.INSTANCE
					.embeddedCollectionToStream(iDatabase, null, iBuffer, null, null, iValue, null, true);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedSet2string", timer);
			break;

		case EMBEDDEDLIST:
			ORecordSerializerSchemaAware2CSV.INSTANCE
					.embeddedCollectionToStream(iDatabase, null, iBuffer, null, null, iValue, null, true);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedList2string", timer);
			break;

		case EMBEDDEDMAP:
			ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapToStream(iDatabase, null, iBuffer, null, null, iValue, null, true);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embedMap2string", timer);
			break;

		case EMBEDDED:
		case CUSTOM:
			// RECORD OR CUSTOM
			OStringSerializerAnyStreamable.INSTANCE.toStream(iDatabase, iBuffer, iValue);
			OProfiler.getInstance().stopChrono("serializer.rec.str.embed2string", timer);
			break;

		default:
			throw new IllegalArgumentException("Type " + iType + " not supported to convert value: " + iValue);
		}
	}

	/**
	 * Parses a string returning the closer type. Numbers by default are INTEGER if haven't decimal separator, otherwise FLOAT. To
	 * treat all the number types numbers are postponed with a character that tells the type: b=byte, s=short, l=long, f=float,
	 * d=double, t=date.
	 * 
	 * @param iUnusualSymbols
	 *          Localized decimal number separators
	 * @param iValue
	 *          Value to parse
	 * @return The closest type recognized
	 */
	public static OType getType(final String iValue) {
		if (iValue.length() == 0)
			return null;

		final char firstChar = iValue.charAt(0);

		if (firstChar == ORID.PREFIX)
			// RID
			return OType.LINK;
		else if (firstChar == '\'' || firstChar == '"')
			return OType.STRING;
		else if (firstChar == OStringSerializerHelper.BINARY_BEGINEND)
			return OType.BINARY;
		else if (firstChar == OStringSerializerHelper.EMBEDDED_BEGIN)
			return OType.EMBEDDED;
		else if (firstChar == OStringSerializerHelper.LINK)
			return OType.LINK;
		else if (firstChar == OStringSerializerHelper.COLLECTION_BEGIN)
			return OType.EMBEDDEDLIST;
		else if (firstChar == OStringSerializerHelper.MAP_BEGIN)
			return OType.EMBEDDEDMAP;
		else if (firstChar == OStringSerializerHelper.CUSTOM_TYPE)
			return OType.CUSTOM;

		// BOOLEAN?
		if (iValue.equalsIgnoreCase("true") || iValue.equalsIgnoreCase("false"))
			return OType.BOOLEAN;

		// NUMBER OR STRING?
		boolean integer = true;
		for (int index = 0; index < iValue.length(); ++index) {
			final char c = iValue.charAt(index);
			if (c < '0' || c > '9')
				if ((index == 0 && (c == '+' || c == '-')))
					continue;
				else if (c == DECIMAL_SEPARATOR)
					integer = false;
				else {
					if (index > 0)
						if (!integer && c == 'E') {
							// CHECK FOR SCIENTIFIC NOTATION
							if (index < iValue.length())
								index++;
							if (iValue.charAt(index) == '-')
								continue;
						} else if (c == 'f')
							return OType.FLOAT;
						else if (c == 'l')
							return OType.LONG;
						else if (c == 'd')
							return OType.DOUBLE;
						else if (c == 'b')
							return OType.BYTE;
						else if (c == 'a')
							return OType.DATE;
						else if (c == 't')
							return OType.DATETIME;
						else if (c == 's')
							return OType.SHORT;

					return OType.STRING;
				}
		}

		if (integer) {
			// AUTO CONVERT TO LONG IF THE INTEGER IS TOO BIG
			final int numberLength = iValue.length();
			if (numberLength > MAX_INTEGER_DIGITS || (numberLength == MAX_INTEGER_DIGITS && iValue.compareTo(MAX_INTEGER_AS_STRING) > 0))
				return OType.LONG;
		}

		return integer ? OType.INTEGER : OType.FLOAT;
	}

	/**
	 * Parses a string returning the value with the closer type. Numbers by default are INTEGER if haven't decimal separator,
	 * otherwise FLOAT. To treat all the number types numbers are postponed with a character that tells the type: b=byte, s=short,
	 * l=long, f=float, d=double, t=date. If starts with # it's a RecordID. Most of the code is equals to getType() but has been
	 * copied to speed-up it.
	 * 
	 * @param iUnusualSymbols
	 *          Localized decimal number separators
	 * @param iValue
	 *          Value to parse
	 * @return The closest type recognized
	 */
	public static Object getTypeValue(final String iValue) {
		if (iValue == null)
			return null;

		if (iValue.length() == 0)
			return "";

		if (iValue.length() > 1)
			if (iValue.charAt(0) == '"' && iValue.charAt(iValue.length() - 1) == '"')
				// STRING
				return OStringSerializerHelper.decode(iValue.substring(1, iValue.length() - 1));
			else if (iValue.charAt(0) == OStringSerializerHelper.BINARY_BEGINEND
					&& iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.BINARY_BEGINEND)
				// STRING
				return OStringSerializerHelper.getBinaryContent(iValue);

		if (iValue.charAt(0) == ORID.PREFIX)
			// RID
			return new ORecordId(iValue);

		boolean integer = true;
		char c;

		for (int index = 0; index < iValue.length(); ++index) {
			c = iValue.charAt(index);
			if (c < '0' || c > '9')
				if ((index == 0 && (c == '+' || c == '-')))
					continue;
				else if (c == DECIMAL_SEPARATOR)
					integer = false;
				else {
					if (index > 0) {
						if (!integer && c == 'E') {
							// CHECK FOR SCIENTIFIC NOTATION
							if (index < iValue.length())
								index++;
							if (iValue.charAt(index) == '-')
								continue;
						}

						final String v = iValue.substring(0, index);

						if (c == 'f')
							return new Float(v);
						else if (c == 'l')
							return new Long(v);
						else if (c == 'd')
							return new Double(v);
						else if (c == 'b')
							return new Byte(v);
						else if (c == 'a' || c == 't')
							return new Date(Long.parseLong(v));
						else if (c == 's')
							return new Short(v);
					}
					return iValue;
				}
		}

		if (integer)
			return new Integer(iValue);
		else
			return new Float(iValue);
	}

	public static void simpleValueToStream(final StringBuilder iBuffer, final OType iType, final Object iValue) {
		if (iValue == null || iType == null)
			return;
		switch (iType) {
		case STRING:
			iBuffer.append('"');
			iBuffer.append(OStringSerializerHelper.encode(iValue.toString()));
			iBuffer.append('"');
			break;

		case BOOLEAN:
			iBuffer.append(String.valueOf(iValue));
			break;

		case INTEGER:
			iBuffer.append(String.valueOf(iValue));
			break;

		case FLOAT:
			iBuffer.append(String.valueOf(iValue));
			iBuffer.append('f');
			break;

		case LONG:
			iBuffer.append(String.valueOf(iValue));
			iBuffer.append('l');
			break;

		case DOUBLE:
			iBuffer.append(String.valueOf(iValue));
			iBuffer.append('d');
			break;

		case SHORT:
			iBuffer.append(String.valueOf(iValue));
			iBuffer.append('s');
			break;

		case BYTE:
			if (iValue instanceof Character)
				iBuffer.append((int) ((Character) iValue).charValue());
			else if (iValue instanceof String)
				iBuffer.append(String.valueOf((int) ((String) iValue).charAt(0)));
			else
				iBuffer.append(String.valueOf(iValue));
			iBuffer.append('b');
			break;

		case BINARY:
			iBuffer.append(OStringSerializerHelper.BINARY_BEGINEND);
			if (iValue instanceof Byte)
				iBuffer.append(OBase64Utils.encodeBytes(new byte[] { ((Byte) iValue).byteValue() }));
			else
				iBuffer.append(OBase64Utils.encodeBytes((byte[]) iValue));
			iBuffer.append(OStringSerializerHelper.BINARY_BEGINEND);
			break;

		case DATE:
			if (iValue instanceof Date) {
				// RESET HOURS, MINUTES, SECONDS AND MILLISECONDS
				Calendar calendar = Calendar.getInstance();
				calendar.setTime((Date) iValue);
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);

				iBuffer.append(calendar.getTimeInMillis());
			} else
				iBuffer.append(iValue);
			iBuffer.append('a');
			break;

		case DATETIME:
			if (iValue instanceof Date)
				iBuffer.append(((Date) iValue).getTime());
			else
				iBuffer.append(iValue);
			iBuffer.append('t');
			break;
		}
	}
}
