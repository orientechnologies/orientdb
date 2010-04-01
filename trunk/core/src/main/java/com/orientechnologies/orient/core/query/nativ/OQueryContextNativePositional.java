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

import com.orientechnologies.orient.core.exception.OQueryExecutionException;
import com.orientechnologies.orient.core.record.ORecordPositional;

@SuppressWarnings("unchecked")
public class OQueryContextNativePositional<T extends ORecordPositional<?>> extends OQueryContextNative<T> {
	public OQueryContextNativePositional column(int iIndex) {
		if (result != null && result.booleanValue())
			return this;

		if (iIndex >= record.size())
			throw new OQueryExecutionException("Column " + iIndex + " not exists. Columns range is: 0-" + (record.size() - 1));

		currentValue = ((ORecordPositional<?>) record).field(iIndex);
		return this;
	}

	@Override
	public OQueryContextNativePositional and() {
		super.and();
		return this;
	}

	@Override
	public OQueryContextNativePositional between(Object iFrom, Object iTo) {
		super.between(iFrom, iTo);
		return this;
	}

	@Override
	public OQueryContextNativePositional different(Object iValue) {
		super.different(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional eq(Object iValue) {
		super.eq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional like(String iValue) {
		super.like(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional major(Object iValue) {
		super.major(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional majorEq(Object iValue) {
		super.majorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional matches(Object iValue) {
		super.matches(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional minor(Object iValue) {
		super.minor(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional minorEq(Object iValue) {
		super.minorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativePositional not() {
		super.not();
		return this;
	}

	@Override
	public OQueryContextNativePositional or() {
		super.or();
		return this;
	}

	@Override
	public OQueryContextNativePositional toChar() {
		super.toChar();
		return this;
	}

	@Override
	public OQueryContextNativePositional toDate() {
		super.toDate();
		return this;
	}

	@Override
	public OQueryContextNativePositional toDouble() {
		super.toDouble();
		return this;
	}

	@Override
	public OQueryContextNativePositional toFloat() {
		super.toFloat();
		return this;
	}

	@Override
	public OQueryContextNativePositional toInt() {
		super.toInt();
		return this;
	}

	@Override
	public OQueryContextNativePositional toLong() {
		super.toLong();
		return this;
	}
}
