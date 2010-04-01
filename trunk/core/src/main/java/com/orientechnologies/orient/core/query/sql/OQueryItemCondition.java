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
package com.orientechnologies.orient.core.query.sql;

import java.util.Collection;

import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

public class OQueryItemCondition {
	private static final String		NULL_VALUE	= "null";
	protected Object							left;
	protected OQueryItemOperator	operator;
	protected Object							right;

	public OQueryItemCondition(Object iLeft, OQueryItemOperator iOperator) {
		this.left = iLeft;
		this.operator = iOperator;
	}

	public OQueryItemCondition(Object iLeft, OQueryItemOperator iOperator, Object iRight) {
		this.left = iLeft;
		this.operator = iOperator;
		this.right = iRight;
	}

	@SuppressWarnings("unchecked")
	public Object evaluate(ORecordSchemaAware<?> iRecord) {
		Object l = evaluate(iRecord, left);
		Object r = evaluate(iRecord, right);

		Object[] convertedValues = checkForConversion(l, r);
		if (convertedValues != null) {
			l = convertedValues[0];
			r = convertedValues[1];
		}

		switch (operator) {
		case EQUALS:
			if (l == null || r == null)
				return false;
			return l.equals(r);

		case NOT_EQUALS:
			if (l == null || r == null)
				return false;
			return !l.equals(r);

		case MINOR:
			if (l == null || r == null)
				return false;
			return ((Comparable) l).compareTo(r) < 0;

		case MINOR_EQUALS:
			if (l == null || r == null)
				return false;
			return ((Comparable) l).compareTo(r) <= 0;

		case MAJOR:
			if (l == null || r == null)
				return false;
			return ((Comparable) l).compareTo(r) > 0;

		case MAJOR_EQUALS:
			if (l == null || r == null)
				return false;
			return ((Comparable) l).compareTo(r) >= 0;

		case AND:
			if (l == null)
				return false;
			return (Boolean) l && (Boolean) r;

		case OR:
			if (l == null)
				return false;
			return (Boolean) l || (Boolean) r;

		case NOT:
			if (l == null)
				return false;
			return !(Boolean) l;

		case LIKE:
			if (l == null || r == null)
				return false;
			return OQueryHelper.like(l.toString(), r.toString());

		case MATCHES:
			if (l == null || r == null)
				return false;
			return l.toString().matches((String) r);

		case IS:
			return l == r;

		case IN:
			if (l == null || r == null)
				return false;

			if (l instanceof Collection<?>) {
				Collection<Object> collection = (Collection<Object>) l;
				for (Object o : collection) {
					if (r.equals(o))
						return true;
				}
			} else if (r instanceof Collection<?>) {

				Collection<Object> collection = (Collection<Object>) r;
				for (Object o : collection) {
					if (l.equals(o))
						return true;
				}
			}
			return false;

		case CONTAINS:
			if (l == null || r == null)
				return false;

			if (l instanceof Collection<?>) {
				Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) l;
				for (ORecordSchemaAware<?> o : collection) {
					if ((Boolean) evaluate(o, right) == Boolean.TRUE)
						return true;
				}
			} else if (r instanceof Collection<?>) {

				Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) r;
				for (ORecordSchemaAware<?> o : collection) {
					if ((Boolean) evaluate(o, left) == Boolean.TRUE)
						return true;
				}
			}
			return false;

		case CONTAINSALL:
			if (l == null || r == null)
				return false;

			if (l instanceof Collection<?>) {
				Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) l;
				Boolean result;
				for (ORecordSchemaAware<?> o : collection) {
					result = (Boolean) evaluate(o, right);
					if (result == Boolean.FALSE)
						return false;
				}
			} else if (r instanceof Collection<?>) {

				Collection<ORecordSchemaAware<?>> collection = (Collection<ORecordSchemaAware<?>>) r;
				Boolean result;
				for (ORecordSchemaAware<?> o : collection) {
					result = (Boolean) evaluate(o, left);
					if (result == Boolean.FALSE)
						return false;
				}
			}
			return true;
		}

		return null;
	}

	private Object[] checkForConversion(Object l, Object r) {
		Object[] result = new Object[] { l, r };

		// INTEGERS
		if (r instanceof Integer && !(l instanceof Integer)) {
			if (l instanceof String && ((String) l).indexOf(".") > -1)
				result[0] = new Float((String) l).intValue();
			else
				result[0] = getInteger(l);
		} else if (l instanceof Integer && !(r instanceof Integer)) {
			if (r instanceof String && ((String) r).indexOf(".") > -1)
				result[1] = new Float((String) r).intValue();
			else
				result[1] = getInteger(r);
		}

		// FLOATS
		else if (r instanceof Float && !(l instanceof Float))
			result[0] = getFloat(l);
		else if (l instanceof Float && !(r instanceof Float))
			result[1] = getFloat(r);
		else
			// AVOID COPY SINCE NO CONVERSION HAPPENED
			return null;

		return result;
	}

	protected Integer getInteger(Object iValue) {
		if (iValue == null)
			return null;

		String stringValue = iValue.toString();

		if (NULL_VALUE.equals(stringValue))
			return null;

		if (stringValue.contains(".") || stringValue.contains(","))
			return (int) Float.parseFloat(stringValue);
		else
			return stringValue.length() > 0 ? new Integer(stringValue) : new Integer(0);
	}

	protected Float getFloat(Object iValue) {
		if (iValue == null)
			return null;

		String stringValue = iValue.toString();

		if (NULL_VALUE.equals(stringValue))
			return null;

		return stringValue.length() > 0 ? new Float(stringValue) : new Float(0);
	}

	protected Object evaluate(ORecordSchemaAware<?> iRecord, Object iValue) {
		if (iValue instanceof OQueryItemValue)
			return ((OQueryItemValue) iValue).getValue(iRecord);

		if (iValue instanceof OQueryItemCondition)
			// NESTED CONDITION: EVALUATE IT RECURSIVELY
			return ((OQueryItemCondition) iValue).evaluate(iRecord);

		// SIMPLE VALUE: JUST RETURN IT
		return iValue;
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();

		buffer.append('(');
		buffer.append(left);
		buffer.append(' ');
		buffer.append(operator);
		buffer.append(' ');
		buffer.append(right);
		buffer.append(')');

		return buffer.toString();
	}
}
