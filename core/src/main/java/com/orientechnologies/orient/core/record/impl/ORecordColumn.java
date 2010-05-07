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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordPositional;
import com.orientechnologies.orient.core.record.ORecordStringable;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;

/**
 * It's schema less. Use this if you need to store Strings at low level. The object can be reused across calls to the database by
 * using the reset() at every re-use.
 */
@SuppressWarnings("unchecked")
public class ORecordColumn extends ORecordAbstract<String> implements ORecordStringable, ORecordPositional<String> {
	protected List<String>		values			= new ArrayList<String>();
	protected char						separator		= OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR;
	protected int							cursor			= 0;

	public static final byte	RECORD_TYPE	= 'c';

	public ORecordColumn() {
		setup();
	}

	public ORecordColumn(ODatabaseRecord<?> iDatabase) {
		super(iDatabase);
		setup();
	}

	public ORecordColumn(ODatabaseRecord<?> iDatabase, byte[] iSource) {
		super(iDatabase, iSource);
		setup();

		extractValues(OBinaryProtocol.bytes2string(iSource));
	}

	public ORecordColumn(ODatabaseRecord<?> iDatabase, ORID iRID) {
		this(iDatabase);
		recordId = (ORecordId) iRID;
	}

	public ORecordColumn value(String iValue) {
		setDirty();
		extractValues(iValue);

		source = null;
		return this;
	}

	@Override
	public ORecordColumn reset() {
		super.reset();
		values.clear();
		cursor = 0;
		return this;
	}

	public ORecordColumn copy() {
		ORecordColumn cloned = new ORecordColumn();
		cloned.source = source;
		cloned.values = values;
		cloned.database = database;
		cloned.recordId = recordId.copy();
		return cloned;
	}

	public ORecordPositional<String> add(Object iValue) {
		setDirty();

		values.add((String) iValue);

		return this;
	}

	public String value() {
		if (values.size() == 0)
			if (source != null)
				extractValues(OBinaryProtocol.bytes2string(source));
		// else
		// // EXTRACT FROM FIELDS
		// extractValues(ORecordFormatStringAbstract.byte2String(storage, toStream()));

		StringBuilder stream = new StringBuilder();
		int pos = 0;
		for (String f : values) {
			if (pos > 0)
				stream.append(separator);

			if (f != null)
				stream.append(f);

			pos++;
		}

		return stream.toString();
	}

	public String field(int iIndex) {
		checkFieldAccess(iIndex);
		return values.get(iIndex);
	}

	public ORecordPositional<String> field(int iIndex, Object iValue) {
		checkFieldAccess(iIndex);
		setDirty();
		values.set(iIndex, iValue != null ? iValue.toString() : null);
		return this;
	}

	public boolean hasNext() {
		return cursor < values.size();
	}

	public String next() {
		return field(cursor++);
	}

	public void remove() {
		throw new UnsupportedOperationException("Remove is not supported");
	}

	public char getSeparator() {
		return separator;
	}

	public void setSeparator(char separator) {
		this.separator = separator;
	}

	@Override
	public String toString() {
		return value();
	}

	@Override
	public byte[] toStream() {
		// JOIN ALL THE VALUES IN A SINGLE STRING USING THE CONFIGURED SEPARATOR
		return OBinaryProtocol.string2bytes(value());
	}

	@Override
	public ORecordColumn fromStream(byte[] iRecordBuffer) {
		super.fromStream(iRecordBuffer);
		extractValues(OBinaryProtocol.bytes2string(source));
		return this;
	}

	public int size() {
		return values.size();
	}

	protected void checkFieldAccess(int iIndex) {
		if (iIndex >= values.size())
			throw new IndexOutOfBoundsException("Index " + iIndex + " is out of range allowed: 0-" + values.size());
	}

	private void extractValues(String iValue) {
		String[] strings = OStringSerializerHelper.split(iValue, separator);

		values.clear();
		for (String s : strings)
			values.add(s);
	}

	@Override
	protected void setup() {
		super.setup();
		recordFormat = ORecordSerializerFactory.instance().getDefaultRecordFormat();
	}

	public byte getRecordType() {
		return RECORD_TYPE;
	}
}
