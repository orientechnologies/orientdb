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

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

@SuppressWarnings("unchecked")
public class OQueryContextNativeSchema<T extends ORecordSchemaAware<?>> extends OQueryContextNative<T> {
	public OQueryContextNativeSchema<T> column(String iName) {
		field(iName);
		return this;
	}

	public OQueryContextNativeSchema<T> field(String iName) {
		if (result != null && result.booleanValue())
			return this;

		T currentRecord = currentValue != null && currentValue instanceof ORecord<?> ? (T) currentValue : record;

		currentValue = currentRecord.field(iName);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> and() {
		super.and();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> between(Object iFrom, Object iTo) {
		super.between(iFrom, iTo);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> different(Object iValue) {
		super.different(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> eq(Object iValue) {
		super.eq(iValue);
		return this;
	}


	@Override
	public OQueryContextNativeSchema<T> like(String iValue) {
		super.like(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> major(Object iValue) {
		super.major(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> majorEq(Object iValue) {
		super.majorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> matches(Object iValue) {
		super.matches(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> minor(Object iValue) {
		super.minor(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> minorEq(Object iValue) {
		super.minorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> not() {
		super.not();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> or() {
		super.or();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> toChar() {
		super.toChar();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> toDate() {
		super.toDate();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> toDouble() {
		super.toDouble();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> toFloat() {
		super.toFloat();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> toInt() {
		super.toInt();
		return this;
	}

	@Override
	public OQueryContextNativeSchema<T> toLong() {
		super.toLong();
		return this;
	}
}
