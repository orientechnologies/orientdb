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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

/**
 * It's schema less. Use this if you need to store Strings at low level. The object can be reused across calls to the database by
 * using the reset() at every re-use.
 */
@SuppressWarnings("unchecked")
public class ORecordFlat extends ORecordAbstract<String> implements ORecordStringable {
	protected String					value;

	public static final byte	RECORD_TYPE	= 'f';

	public ORecordFlat() {
		setup();
	}

	public ORecordFlat(ODatabaseRecord<?> iDatabase) {
		super(iDatabase);
		setup();
	}

	public ORecordFlat(ODatabaseRecord<?> iDatabase, byte[] iSource) {
		super(iDatabase, iSource);
		setup();
	}

	public ORecordFlat(ODatabaseRecord<?> iDatabase, ORID iRID) {
		this(iDatabase);
		recordId = (ORecordId) iRID;
	}

	public ORecordFlat value(String iValue) {
		value = iValue;
		source = null;
		setDirty();
		return this;
	}

	@Override
	public ORecordFlat reset() {
		super.reset();
		value = null;
		return this;
	}

	public ORecordFlat copy() {
		ORecordFlat cloned = new ORecordFlat();
		cloned.source = source;
		cloned.value = value;
		cloned.database = database;
		cloned.recordId = recordId.copy();
		setDirty();
		return cloned;
	}

	public String value() {
		if (value == null) {
			// LAZY DESERIALIZATION
			if (source == null && getIdentity() != null && getIdentity().isValid())
				load();

			// LAZY LOADING: LOAD THE RECORD FIRST
			value = OBinaryProtocol.bytes2string(source);
		}

		return value;
	}

	@Override
	public String toString() {
		return value();
	}

	@Override
	public byte[] toStream() {
		return OBinaryProtocol.string2bytes(value());
	}

	@Override
	public ORecordFlat fromStream(byte[] iRecordBuffer) {
		super.fromStream(iRecordBuffer);
		return this;
	}

	public int size() {
		final String v = value();
		return v != null ? v.length() : 0;
	}

	public byte getRecordType() {
		return RECORD_TYPE;
	}
}
