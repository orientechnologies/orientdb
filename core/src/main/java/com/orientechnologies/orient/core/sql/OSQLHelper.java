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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorAnd;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContains;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorContainsAll;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIn;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorIs;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorLike;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMajorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinor;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorMinorEquals;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorNot;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorOr;
import com.orientechnologies.orient.core.sql.operator.OQueryOperatorTraverse;

/**
 * SQL Helper class
 * 
 * @author luca
 * 
 */
public class OSQLHelper {
	public static final String			NAME						= "sql";

	public static final String			CLUSTER_PREFIX	= "CLUSTER:";
	public static final String			CLASS_PREFIX		= "CLASS:";

	public static final String			KEYWORD_SELECT	= "SELECT";
	public static final String			KEYWORD_INSERT	= "INSERT";
	public static final String			KEYWORD_UPDATE	= "UPDATE";
	public static final String			KEYWORD_DELETE	= "DELETE";
	public static final String			KEYWORD_FROM		= "FROM";
	public static final String			KEYWORD_WHERE		= "WHERE";
	public static final String			KEYWORD_COLUMN	= "COLUMN";

	public static OQueryOperator[]	OPERATORS				= { new OQueryOperatorAnd(), new OQueryOperatorOr(), new OQueryOperatorNot(),
			new OQueryOperatorEquals(), new OQueryOperatorMinorEquals(), new OQueryOperatorMinor(), new OQueryOperatorMajorEquals(),
			new OQueryOperatorContainsAll(), new OQueryOperatorMajor(), new OQueryOperatorLike(), new OQueryOperatorIs(),
			new OQueryOperatorIn(), new OQueryOperatorContains(), new OQueryOperatorTraverse() };

	public static int jumpWhiteSpaces(final String iText, int iCurrentPosition) {
		for (; iCurrentPosition < iText.length(); ++iCurrentPosition)
			if (!Character.isWhitespace(iText.charAt(iCurrentPosition)))
				break;
		return iCurrentPosition;
	}

	public static int nextWord(final String iText, final String iTextUpperCase, int ioCurrentPosition, final StringBuilder ioWord,
			final boolean iForceUpperCase) {
		ioCurrentPosition = OSQLHelper.jumpWhiteSpaces(iText, ioCurrentPosition);
		if (ioCurrentPosition >= iText.length())
			return -1;

		int begin = ioCurrentPosition;

		for (; ioCurrentPosition < iText.length(); ++ioCurrentPosition)
			if (Character.isWhitespace(iText.charAt(ioCurrentPosition)))
				break;

		ioWord.setLength(0);
		ioWord.append(iForceUpperCase ? iTextUpperCase.substring(begin, ioCurrentPosition) : iText.substring(begin, ioCurrentPosition));

		return ioCurrentPosition;
	}

	public static OQueryOperator[] getOperators() {
		return OPERATORS;
	}

	public static void registerOperator(final OQueryOperator iOperator) {
		OQueryOperator[] ops = new OQueryOperator[OPERATORS.length + 1];
		System.arraycopy(OPERATORS, 0, ops, 0, OPERATORS.length);
		OPERATORS = ops;
	}

	/**
	 * Convert fields from text to real value. Supports: String, RID, Float and Integer.
	 * 
	 * @param values
	 * @return
	 */
	public static Object[] convertValues(String[] values) {
		String value;
		Object fieldValue;

		Object[] fieldValues = new Object[values.length];

		for (int i = 0; i < values.length; ++i) {
			value = values[i];
			fieldValue = null;

			if (value != null) {
				if (value.startsWith("'") && value.endsWith("'"))
					// STRING
					fieldValue = value.substring(1, value.length() - 1);
				else if (value.indexOf(":") > -1)
					// RID
					fieldValue = new ORecordId(value);
				else {
					// NUMBER
					if (value.contains("."))
						// FLOAT/DOUBLE
						fieldValue = new Float(value);
					else
						fieldValue = new Integer(value);
				}
			}

			fieldValues[i] = fieldValue;
		}

		return fieldValues;
	}
}
