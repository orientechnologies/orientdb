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
package com.orientechnologies.orient.core.sql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerCSVAbstract;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;

/**
 * SQL Helper class
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLHelper {
	public static final String	NAME							= "sql";

	public static final String	VALUE_NOT_PARSED	= "_NOT_PARSED_";
	public static final String	NOT_NULL					= "_NOT_NULL_";
	public static final String	DEFINED						= "_DEFINED_";

	public static int nextWord(final String iText, final String iTextUpperCase, int ioCurrentPosition, final StringBuilder ioWord,
			final boolean iForceUpperCase) {
		return nextWord(iText, iTextUpperCase, ioCurrentPosition, ioWord, iForceUpperCase, " =><()");
	}

	public static int nextWord(final String iText, final String iTextUpperCase, int ioCurrentPosition, final StringBuilder ioWord,
			final boolean iForceUpperCase, final String iSeparatorChars) {
		ioWord.setLength(0);

		ioCurrentPosition = OStringParser.jumpWhiteSpaces(iText, ioCurrentPosition);
		if (ioCurrentPosition < 0)
			return -1;

		final String word = OStringParser.getWordFromString(iForceUpperCase ? iTextUpperCase : iText, ioCurrentPosition,
				iSeparatorChars);

		if (word != null && word.length() > 0) {
			ioWord.append(word);
			ioCurrentPosition += word.length();
		}

		return ioCurrentPosition;
	}

	/**
	 * Convert fields from text to real value. Supports: String, RID, Boolean, Float, Integer and NULL.
	 * 
	 * @param iDatabase
	 * @param iValue
	 *          Value to convert.
	 * @return The value converted if recognized, otherwise VALUE_NOT_PARSED
	 */
	public static Object parseValue(final ODatabaseRecord iDatabase, String iValue) {
		if (iValue == null)
			return null;

		iValue = iValue.trim();

		Object fieldValue = VALUE_NOT_PARSED;

		if (iValue.startsWith("'") && iValue.endsWith("'") || iValue.startsWith("\"") && iValue.endsWith("\""))
			// STRING
			fieldValue = stringContent(iValue);
		else if (iValue.charAt(0) == OStringSerializerHelper.COLLECTION_BEGIN
				&& iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.COLLECTION_END) {
			// COLLECTION/ARRAY
			final List<String> items = OStringSerializerHelper.smartSplit(iValue.substring(1, iValue.length() - 1),
					OStringSerializerHelper.RECORD_SEPARATOR);

			final List<Object> coll = new ArrayList<Object>();
			for (String item : items) {
				coll.add(parseValue(iDatabase, item));
			}
			fieldValue = coll;

		} else if (iValue.indexOf(':') > 0)
			// RID
			fieldValue = new ORecordId(iValue.trim());
		else {

			final String upperCase = iValue.toUpperCase();
			if (upperCase.equals("NULL"))
				// NULL
				fieldValue = null;
			else if (upperCase.equals("NOT NULL"))
				// NULL
				fieldValue = NOT_NULL;
			else if (upperCase.equals("DEFINED"))
				// NULL
				fieldValue = DEFINED;
			else if (upperCase.equals("TRUE"))
				// BOOLEAN, TRUE
				fieldValue = Boolean.TRUE;
			else if (upperCase.equals("FALSE"))
				// BOOLEAN, FALSE
				fieldValue = Boolean.FALSE;
			else {
				final Object v = parseStringNumber(iValue);
				if (v != null)
					fieldValue = v;
			}
		}

		return fieldValue;
	}

	public static Object parseStringNumber(final String iValue) {
		final OType t = ORecordSerializerCSVAbstract.getType(iValue);

		if (t == OType.INTEGER)
			return Integer.parseInt(iValue);
		else if (t == OType.LONG)
			return Long.parseLong(iValue);
		else if (t == OType.FLOAT)
			return Float.parseFloat(iValue);
		else if (t == OType.SHORT)
			return Short.parseShort(iValue);
		else if (t == OType.BYTE)
			return Byte.parseByte(iValue);
		else if (t == OType.DOUBLE)
			return Double.parseDouble(iValue);
		else if (t == OType.DATE || t == OType.DATETIME)
			return new Date(Long.parseLong(iValue));

		return null;
	}

	public static Object parseValue(final OSQLFilter iSQLFilter, final ODatabaseRecord iDatabase, final OCommandToParse iCommand,
			final String iWord) {
		if (iWord.charAt(0) == OStringSerializerHelper.PARAMETER_POSITIONAL
				|| iWord.charAt(0) == OStringSerializerHelper.PARAMETER_NAMED) {
			if (iSQLFilter != null)
				return iSQLFilter.addParameter(iWord);
			else
				return new OSQLFilterItemParameter(iWord);
		} else
			return parseValue(iDatabase, iCommand, iWord);
	}

	public static Object parseValue(final ODatabaseRecord iDatabase, final OCommandToParse iCommand, final String iWord) {
		if (iWord.equals("*"))
			return "*";

		// TRY TO PARSE AS RAW VALUE
		final Object v = parseValue(iDatabase, iWord);
		if (v != VALUE_NOT_PARSED)
			return v;

		// TRY TO PARSE AS FUNCTION
		final Object func = OSQLHelper.getFunction(iDatabase, iCommand, iWord);
		if (func != null)
			return func;

		// PARSE AS FIELD
		return new OSQLFilterItemField(iCommand, iWord);
	}

	public static String stringContent(final String iContent) {
		if (iContent.length() > 1 && iContent.startsWith("'") || iContent.startsWith("\""))
			return iContent.substring(1, iContent.length() - 1);
		return iContent;
	}

	public static Object getFunction(final ODatabaseRecord database, final OCommandToParse iCommand, final String iWord) {
		int sepPos = iWord.indexOf('.');
		int parPos = iWord.indexOf(OStringSerializerHelper.PARENTHESIS_BEGIN);

		if (Character.isLetter(iWord.charAt(0)) && parPos > -1 && (sepPos == -1 || sepPos > parPos)) {
			// SEARCH FOR THE FUNCTION
			final String funcName = iWord.substring(0, parPos);

			final List<String> funcParamsText = OStringSerializerHelper.getParameters(iWord);

			OSQLFunction function = OSQLEngine.getInstance().getInlineFunction(funcName);
			if (function == null)
				// AGGREGATION ?
				function = OSQLEngine.getInstance().getAggregationFunction(funcName);

			if (function == null)
				throw new OCommandSQLParsingException("Unknow function " + funcName + "()");

			if (function.getMinParams() > -1 && funcParamsText.size() < function.getMinParams() || function.getMaxParams() > -1
					&& funcParamsText.size() > function.getMaxParams())
				throw new IllegalArgumentException("Syntax error. Expected: " + function.getSyntax());

			// PARSE PARAMETERS
			final Object[] funcParams = new Object[funcParamsText.size()];
			for (int i = 0; i < funcParamsText.size(); ++i) {
				funcParams[i] = OSQLHelper.parseValue(database, iCommand, funcParamsText.get(i));
			}

			// FUNCTION: CRETAE A RUN-TIME CONTAINER FOR IT TO SAVE THE PARAMETERS
			return new OSQLFunctionRuntime(function, funcParams);
		}

		return null;
	}

	public static Object getValue(final Object iObject) {
		if (iObject == null)
			return null;

		if (iObject instanceof OSQLFilterItem)
			return ((OSQLFilterItem) iObject).getValue(null);

		return iObject;
	}

	public static Object getValue(final Object iObject, final ORecordInternal<?> iRecord) {
		if (iObject == null)
			return null;

		if (iObject instanceof OSQLFilterItem)
			return ((OSQLFilterItem) iObject).getValue(iRecord);

		return iObject;
	}

	public static void bindParameters(final ODocument iDocument, final Map<String, Object> iFields, final Map<Object, Object> iArgs) {
		int paramCounter = 0;

		// BIND VALUES
		for (Entry<String, Object> field : iFields.entrySet()) {
			if (field.getValue() instanceof OSQLFilterItemField) {
				final OSQLFilterItemField f = (OSQLFilterItemField) field.getValue();
				if (f.getName().equals("?"))
					// POSITIONAL PARAMETER
					iDocument.field(field.getKey(), iArgs.get(paramCounter++));
				else if (f.getName().startsWith(":"))
					// NAMED PARAMETER
					iDocument.field(field.getKey(), iArgs.get(f.getName().substring(1)));
			} else
				iDocument.field(field.getKey(), OSQLHelper.getValue(field.getValue(), iDocument));
		}

	}
}
