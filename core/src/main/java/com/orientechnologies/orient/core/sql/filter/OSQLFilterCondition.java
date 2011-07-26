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
package com.orientechnologies.orient.core.sql.filter;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.regex.Pattern;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.OQueryRuntimeValueMulti;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.operator.OQueryOperator;

/**
 * Run-time query condition evaluator.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterCondition {
	private static final String	NULL_VALUE	= "null";
	protected Object						left;
	protected OQueryOperator		operator;
	protected Object						right;

	public OSQLFilterCondition(final Object iLeft, final OQueryOperator iOperator) {
		this.left = iLeft;
		this.operator = iOperator;
	}

	public OSQLFilterCondition(final Object iLeft, final OQueryOperator iOperator, final Object iRight) {
		this.left = iLeft;
		this.operator = iOperator;
		this.right = iRight;
	}

	public Object evaluate(final ORecordSchemaAware<?> iRecord) {
		Object l = evaluate(iRecord, left);
		Object r = evaluate(iRecord, right);

		final Object[] convertedValues = checkForConversion(iRecord, l, r);
		if (convertedValues != null) {
			l = convertedValues[0];
			r = convertedValues[1];
		}

		if (operator == null)
			// UNITARY OPERATOR: JUST RETURN LEFT RESULT
			return l;

		return operator.evaluateRecord(iRecord, this, l, r);
	}

	private Object[] checkForConversion(final ORecordSchemaAware<?> iRecord, final Object l, final Object r) {
		Object[] result = null;

		// DEFINED OPERATOR
		if ((r instanceof String && r.equals(OSQLHelper.DEFINED)) || (l instanceof String && l.equals(OSQLHelper.DEFINED))) {
			result = new Object[] { ((OSQLFilterItemAbstract) this.left).getName(), r };
		}

		else if (l != null && r != null && !l.getClass().isAssignableFrom(r.getClass()) && !r.getClass().isAssignableFrom(l.getClass()))
			// INTEGERS
			if (r instanceof Integer && !(l instanceof Number)) {
				if (l instanceof String && ((String) l).indexOf('.') > -1)
					result = new Object[] { new Float((String) l).intValue(), r };
				else if (l instanceof Date)
					result = new Object[] { ((Date) l).getTime(), r };
				else if (!(l instanceof OQueryRuntimeValueMulti) && !(l instanceof Collection<?>) && !l.getClass().isArray())
					result = new Object[] { getInteger(l), r };
			} else if (l instanceof Integer && !(r instanceof Number)) {
				if (r instanceof String && ((String) r).indexOf('.') > -1)
					result = new Object[] { l, new Float((String) r).intValue() };
				else if (r instanceof Date)
					result = new Object[] { l, ((Date) r).getTime() };
				else if (!(r instanceof OQueryRuntimeValueMulti) && !(r instanceof Collection<?>) && !r.getClass().isArray())
					result = new Object[] { l, getInteger(r) };
			}

			// FLOATS
			else if (r instanceof Float && !(l instanceof Float))
				result = new Object[] { getFloat(l), r };
			else if (l instanceof Float && !(r instanceof Float))
				result = new Object[] { l, getFloat(r) };

			// DATES
			else if (r instanceof Date && !(l.getClass().isArray() || l instanceof Date)) {
				result = new Object[] { getDate(iRecord, l), r };
			} else if (l instanceof Date && !(r.getClass().isArray() || r instanceof Date)) {
				result = new Object[] { l, getDate(iRecord, r) };
			}

			// RIDS
			else if (r instanceof ORID && l instanceof String && !l.equals(OSQLHelper.NOT_NULL)) {
				result = new Object[] { new ORecordId((String) l), r };
			} else if (l instanceof ORID && r instanceof String && !r.equals(OSQLHelper.NOT_NULL)) {
				result = new Object[] { l, new ORecordId((String) r) };
			}

		return result;
	}

	protected Integer getInteger(Object iValue) {
		if (iValue == null)
			return null;

		final String stringValue = iValue.toString();

		if (NULL_VALUE.equals(stringValue))
			return null;
		if (OSQLHelper.DEFINED.equals(stringValue))
			return null;

		if (OStringSerializerHelper.contains(stringValue, '.') || OStringSerializerHelper.contains(stringValue, ','))
			return (int) Float.parseFloat(stringValue);
		else
			return stringValue.length() > 0 ? new Integer(stringValue) : new Integer(0);
	}

	protected Float getFloat(final Object iValue) {
		if (iValue == null)
			return null;

		final String stringValue = iValue.toString();

		if (NULL_VALUE.equals(stringValue))
			return null;

		return stringValue.length() > 0 ? new Float(stringValue) : new Float(0);
	}

	protected Date getDate(final ORecordSchemaAware<?> iRecord, final Object iValue) {
		if (iValue == null)
			return null;

		if (iValue instanceof Long)
			return new Date(((Long) iValue).longValue());

		String stringValue = iValue.toString();

		if (NULL_VALUE.equals(stringValue))
			return null;

		if (stringValue.length() <= 0)
			return null;

		if (Pattern.matches("^\\d+$", stringValue)) {
			return new Date(Long.valueOf(stringValue).longValue());
		}

		final OStorageConfiguration config = iRecord.getDatabase().getStorage().getConfiguration();

		DateFormat formatter = config.getDateFormatInstance();

		if (stringValue.length() > config.dateFormat.length()) {
			// ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
			formatter = config.getDateTimeFormatInstance();
		}

		try {
			return formatter.parse(stringValue);
		} catch (ParseException pe) {
			throw new OQueryParsingException("Error on conversion of date '" + stringValue + "' using the format: "
					+ formatter.toString());
		}
	}

	protected Object evaluate(ORecordSchemaAware<?> iRecord, final Object iValue) {
		if (iValue instanceof OSQLFilterItem) {
			if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
				try {
					iRecord = (ORecordSchemaAware<?>) iRecord.load();
				} catch (ORecordNotFoundException e) {
					return null;
				}
			}

			return ((OSQLFilterItem) iValue).getValue(iRecord);
		}

		if (iValue instanceof OSQLFilterCondition)
			// NESTED CONDITION: EVALUATE IT RECURSIVELY
			return ((OSQLFilterCondition) iValue).evaluate(iRecord);

		if (iValue instanceof OSQLFunctionRuntime) {
			// STATELESS FUNCTION: EXECUTE IT
			final OSQLFunctionRuntime f = (OSQLFunctionRuntime) iValue;
			return f.execute(iRecord);
		}

		// SIMPLE VALUE: JUST RETURN IT
		return iValue;
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();

		buffer.append('(');
		buffer.append(left);
		if (operator != null) {
			buffer.append(' ');
			buffer.append(operator);
			buffer.append(' ');
			buffer.append(right);
			buffer.append(')');
		}

		return buffer.toString();
	}

	public Object getLeft() {
		return left;
	}

	public Object getRight() {
		return right;
	}

	public OQueryOperator getOperator() {
		return operator;
	}

	public void setLeft(final Object iValue) {
		left = iValue;
	}

	public void setRight(final Object iValue) {
		right = iValue;
	}
}
