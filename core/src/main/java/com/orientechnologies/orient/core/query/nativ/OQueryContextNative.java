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

import java.util.Collection;
import java.util.Date;
import java.util.Map;

import com.orientechnologies.orient.core.query.OQueryContext;
import com.orientechnologies.orient.core.query.OQueryHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings("unchecked")
public class OQueryContextNative extends OQueryContext {
	protected Boolean	finalResult;
	protected Boolean	partialResult;
	protected Object	currentValue;

	public OQueryContextNative column(final String iName) {
		field(iName);
		return this;
	}

	@Override
	public void setRecord(final ODocument iRecord) {
		super.setRecord(iRecord);

		finalResult = null;
		partialResult = null;
		currentValue = null;
	}

	public OQueryContextNative field(final String iName) {
		if (finalResult != null && finalResult.booleanValue())
			return this;

		if (iName == null)
			// ERROR: BREAK CHAIN
			return error();

		final ODocument currentRecord = currentValue != null && currentValue instanceof ODocument ? (ODocument) currentValue
				: initialRecord;

		currentValue = currentRecord.field(iName);
		return this;
	}

	/**
	 * Sets as current value the map's value by key.
	 * 
	 * @param iKey
	 *           Key to use for the lookup
	 * @return This object to allow fluent expressions in chain.
	 */
	public OQueryContextNative key(final Object iKey) {
		if (finalResult != null && finalResult.booleanValue())
			return this;

		if (iKey == null)
			// ERROR: BREAK CHAIN
			return error();

		if (currentValue != null && currentValue instanceof Map)
			currentValue = ((Map<Object, Object>) currentValue).get(iKey);

		return this;
	}

	/**
	 * Sets as current value TRUE if the key exists, otherwise FALSE. For Maps and Collections the contains() method is called.
	 * Against Documents containsField() is invoked.
	 * 
	 * 
	 * @param iKey
	 *           Key to use for the lookup
	 * @return This object to allow fluent expressions in chain.
	 */
	public OQueryContextNative contains(final Object iKey) {
		if (finalResult != null && finalResult.booleanValue())
			return this;

		if (iKey == null)
			// ERROR: BREAK CHAIN
			return error();

		if (currentValue != null)
			if (currentValue instanceof Map)
				currentValue = ((Map<Object, Object>) currentValue).containsKey(iKey);
			else if (currentValue instanceof Collection)
				currentValue = ((Collection<Object>) currentValue).contains(iKey);
			else if (currentValue instanceof ODocument)
				currentValue = ((ODocument) currentValue).containsField(iKey.toString());

		return this;
	}

	/**
	 * Executes logical AND operator between left and right conditions.
	 * 
	 * @return This object to allow fluent expressions in chain.
	 */
	public OQueryContextNative and() {
		if (finalResult == null) {
			if (partialResult != null && !partialResult)
				finalResult = false;
		}
		return this;
	}

	/**
	 * Executes logical OR operator between left and right conditions.
	 * 
	 * @return This object to allow fluent expressions in chain.
	 */
	public OQueryContextNative or() {
		if (finalResult == null) {
			if (partialResult != null && partialResult)
				finalResult = true;
		}
		return this;
	}

	public OQueryContextNative not() {
		if (finalResult == null) {
			if (partialResult != null)
				partialResult = !partialResult;
		}
		return this;
	}

	public OQueryContextNative like(final String iValue) {
		if (checkOperator())
			partialResult = OQueryHelper.like(currentValue.toString(), iValue);

		return this;
	}

	public OQueryContextNative matches(final Object iValue) {
		if (checkOperator())
			partialResult = currentValue.toString().matches(iValue.toString());
		return this;
	}

	public OQueryContextNative eq(final Object iValue) {
		if (checkOperator())
			partialResult = currentValue.equals(iValue);
		return this;
	}

	public OQueryContextNative different(final Object iValue) {
		if (checkOperator())
			partialResult = !currentValue.equals(iValue);
		return this;
	}

	public OQueryContextNative between(final Object iFrom, final Object iTo) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iFrom) >= 0
					&& ((Comparable<Object>) currentValue).compareTo(iTo) <= 0;
		return this;
	}

	public OQueryContextNative minor(final Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) < 0;
		return this;
	}

	public OQueryContextNative minorEq(final Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) <= 0;
		return this;
	}

	public OQueryContextNative major(final Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) > 0;
		return this;
	}

	public OQueryContextNative majorEq(final Object iValue) {
		if (checkOperator())
			partialResult = ((Comparable<Object>) currentValue).compareTo(iValue) >= 0;
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
		return finalResult != null ? finalResult : partialResult != null ? partialResult : false;
	}

	protected boolean checkOperator() {
		if (finalResult != null)
			return false;

		if (currentValue == null) {
			finalResult = false;
			return false;
		}

		return true;
	}

	/**
	 * Breaks the chain.
	 */
	protected OQueryContextNative error() {
		currentValue = null;
		finalResult = Boolean.FALSE;
		partialResult = null;
		return this;
	}
}
