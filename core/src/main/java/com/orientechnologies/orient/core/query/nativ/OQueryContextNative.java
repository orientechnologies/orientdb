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

import com.orientechnologies.orient.core.query.OQueryContext;
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

	public OQueryContextNative<T> and() {
		if (result == null) {
			if (partialResult != null && !partialResult)
				result = partialResult;
		}
		return this;
	}

	public OQueryContextNative<T> or() {
		if (result == null) {
			if (partialResult != null && partialResult)
				result = partialResult;
		}
		return this;
	}

	public OQueryContextNative<T> not() {
		if (result == null) {
			if (partialResult != null)
				partialResult = !partialResult;
		}
		return this;
	}

	public OQueryContextNative<T> like(String iValue) {
		if (checkOperator())
			partialResult = OQueryHelper.like(currentValue.toString(), iValue);

		return this;
	}

	public OQueryContextNative<T> matches(Object iValue) {
		if (checkOperator())
			partialResult = currentValue.toString().matches(iValue.toString());
		return this;
	}

	public OQueryContextNative<T> eq(Object iValue) {
		if (checkOperator())
			partialResult = currentValue.equals(iValue);
		return this;
	}

	public OQueryContextNative<T> different(Object iValue) {
		if (checkOperator())
			partialResult = !currentValue.equals(iValue);
		return this;
	}

	public OQueryContextNative<T> between(Object iFrom, Object iTo) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iFrom) >= 0
					&& ((Comparable<Object>) currentValue).compareTo(iTo) <= 0;
		return this;
	}

	public OQueryContextNative<T> minor(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) < 0;
		return this;
	}

	public OQueryContextNative<T> minorEq(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) <= 0;
		return this;
	}

	public OQueryContextNative<T> major(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) > 0;
		return this;
	}

	public OQueryContextNative<T> majorEq(Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) >= 0;
		return this;
	}

	public OQueryContextNative<T> toInt() {
		if (checkOperator())
			currentValue = Integer.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative<T> toLong() {
		if (checkOperator())
			currentValue = Long.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative<T> toFloat() {
		if (checkOperator())
			currentValue = Float.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative<T> toDouble() {
		if (checkOperator())
			currentValue = Double.valueOf(currentValue.toString());
		return this;
	}

	public OQueryContextNative<T> toChar() {
		if (checkOperator())
			currentValue = currentValue.toString().charAt(0);
		return this;
	}

	public OQueryContextNative<T> toDate() {
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

		if (currentValue == null) {
			result = false;
			return false;
			// throw new OCommandExecutionException("Can't execute operators if no value was selected. Use column() to obtain a value");
		}

		return true;
	}
}
