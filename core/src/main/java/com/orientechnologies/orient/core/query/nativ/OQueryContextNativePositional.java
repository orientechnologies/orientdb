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

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordPositional;

@SuppressWarnings("unchecked")
public class OQueryContextNativePositional<T extends ORecordPositional<?>> extends OQueryContextNative<T> {
	public OQueryContextNativePositional<T> column(final int iIndex) {
		if (result != null && result.booleanValue())
			return this;

		T currentRecord = currentValue != null && currentValue instanceof ORecord<?> ? (T) currentValue : record;

		if (iIndex >= currentRecord.size())
			throw new OCommandExecutionException("Column " + iIndex + " not exists. Columns range is: 0-" + (currentRecord.size() - 1));

		currentValue = currentRecord.field(iIndex);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> and() {
		super.and();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> between(final Object iFrom, final Object iTo) {
		super.between(iFrom, iTo);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> different(final Object iValue) {
		super.different(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> eq(final Object iValue) {
		super.eq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> like(final String iValue) {
		super.like(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> major(final Object iValue) {
		super.major(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> majorEq(final Object iValue) {
		super.majorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> matches(final Object iValue) {
		super.matches(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> minor(final Object iValue) {
		super.minor(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> minorEq(final Object iValue) {
		super.minorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> not() {
		super.not();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> or() {
		super.or();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> toChar() {
		super.toChar();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> toDate() {
		super.toDate();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> toDouble() {
		super.toDouble();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> toFloat() {
		super.toFloat();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> toInt() {
		super.toInt();
		return this;
	}

	@Override
	public OQueryContextNativePositional<T> toLong() {
		super.toLong();
		return this;
	}
}
