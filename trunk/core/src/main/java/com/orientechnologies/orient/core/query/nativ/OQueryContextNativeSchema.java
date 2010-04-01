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

import com.orientechnologies.orient.core.record.ORecordSchemaAware;

@SuppressWarnings("unchecked")
public class OQueryContextNativeSchema<T extends ORecordSchemaAware<?>> extends OQueryContextNative<T> {
	public OQueryContextNativeSchema column(String iName) {
		if (result != null && result.booleanValue())
			return this;

		currentValue = record.field(iName);
		return this;
	}

	@Override
	public OQueryContextNativeSchema and() {
		super.and();
		return this;
	}

	@Override
	public OQueryContextNativeSchema between(Object iFrom, Object iTo) {
		super.between(iFrom, iTo);
		return this;
	}

	@Override
	public OQueryContextNativeSchema different(Object iValue) {
		super.different(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema eq(Object iValue) {
		super.eq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema like(String iValue) {
		super.like(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema major(Object iValue) {
		super.major(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema majorEq(Object iValue) {
		super.majorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema matches(Object iValue) {
		super.matches(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema minor(Object iValue) {
		super.minor(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema minorEq(Object iValue) {
		super.minorEq(iValue);
		return this;
	}

	@Override
	public OQueryContextNativeSchema not() {
		super.not();
		return this;
	}

	@Override
	public OQueryContextNativeSchema or() {
		super.or();
		return this;
	}

	@Override
	public OQueryContextNativeSchema toChar() {
		super.toChar();
		return this;
	}

	@Override
	public OQueryContextNativeSchema toDate() {
		super.toDate();
		return this;
	}

	@Override
	public OQueryContextNativeSchema toDouble() {
		super.toDouble();
		return this;
	}

	@Override
	public OQueryContextNativeSchema toFloat() {
		super.toFloat();
		return this;
	}

	@Override
	public OQueryContextNativeSchema toInt() {
		super.toInt();
		return this;
	}

	@Override
	public OQueryContextNativeSchema toLong() {
		super.toLong();
		return this;
	}
}
