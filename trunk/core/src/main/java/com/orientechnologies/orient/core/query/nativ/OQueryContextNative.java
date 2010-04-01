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
package com.orientechnologies.orient.core.query.nativ;

import java.util.Date;

import com.orientechnologies.orient.core.exception.OQueryExecutionException;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.ORecordInternal;

@SuppressWarnings("unchecked")
public class OQueryContextNative<T extends ORecordInternal<?>> extends OQueryContext<T> {
	protected Boolean	result;
	protected Boolean	partialResult;
	protected Object	currentValue;

	@Override
	public void setRecord(T iRecord) {
		super.setRecord(iRecord);

		result = null;
		partialResult = null;
		currentValue = null;
	}

	public OQueryContextNative and() {
		if (result == null) {
			if (partialResult != null && !partialResult)
				result = partialResult;
		}
		return this;
	}

	public OQueryContextNative or() {
		if (result == null) {
			if (partialResult != null && partialResult)
				result = partialResult;
		}
		return this;
	}

	public OQueryContextNative not() {
		if (result == null) {
			if (partialResult != null)
				partialResult = !partialResult;
		}
		return this;
	}

	public OQueryContextNative like(String iValue) {
		if (checkOperator())
			partialResult = OQueryHelper.like(currentValue.toString(), iValue);

		return this;
	}

	public OQueryContextNative matches(Object iValue) {
		if (checkOperator())
			partialResult = currentValue.toString().matches(iValue.toString());
		return this;
	}

	public OQueryContextNative eq(Object iValue) {
		if (checkOperator())
			partialResult = currentValue.equals(iValue);
		return this;
	}

	public OQueryContextNative different(Object iValue) {
		if (checkOperator())
			partialResult = !currentValue.equals(iValue);
		return this;
	}

	public OQueryContextNative between(Object iFrom, Object iTo) {
		if (checkOperator())
			partialResult = ((Comparable) currentValue).compareTo(iFrom) >= 0 && ((Comparable) currentValue).compareTo(iTo) <= 0;
		return this;
	}

	public OQueryContextNative minor(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable) currentValue).compareTo(iValue) < 0;
		return this;
	}

	public OQueryContextNative minorEq(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable) currentValue).compareTo(iValue) <= 0;
		return this;
	}

	public OQueryContextNative major(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable) currentValue).compareTo(iValue) > 0;
		return this;
	}

	public OQueryContextNative majorEq(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable) currentValue).compareTo(iValue) >= 0;
		return this;
	}

	public OQueryContextNative toInt() {
		if (checkOperator())
			currentValue = Integer.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative toLong() {
		if (checkOperator())
			currentValue = Long.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative toFloat() {
		if (checkOperator())
			currentValue = Float.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative toDouble() {
		if (checkOperator())
			currentValue = Double.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative toChar() {
		if (checkOperator())
			currentValue = currentValue.toString().charAt(0);
		return this;
	}

	public OQueryContextNative toDate() {
		if (checkOperator())
			currentValue = new Date(Long.valueOf(currentValue.toString()));
		return this;
	}

	public boolean go() {
		return result != null ? result : partialResult != null ? partialResult : false;
	}

	protected boolean checkOperator() {
		if (result != null)
			return false;

		if (currentValue == null)
			throw new OQueryExecutionException("Can't execute operators if no value was selected. Use column() to obtain a value");

		return true;
	}
}
