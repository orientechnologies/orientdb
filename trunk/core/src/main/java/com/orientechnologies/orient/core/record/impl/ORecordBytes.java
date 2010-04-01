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

/**
 * The rawest representation of a record. It's schema less. Use this if you need to store Strings or byte[] without matter about the
 * content. Useful also to store multimedia contents and binary files. The object can be reused across calls to the database by
 * using the reset() at every re-use.
 */
@SuppressWarnings("unchecked")
public class ORecordBytes extends ORecordAbstract<byte[]> {
	public ORecordBytes() {
		setup();
	}

	public ORecordBytes(ODatabaseRecord<?> iDatabase) {
		super(iDatabase);
		setup();
	}

	public ORecordBytes(ODatabaseRecord<?> iDatabase, byte[] iSource) {
		super(iDatabase, iSource);
		setup();
	}

	public ORecordBytes(ODatabaseRecord<?> iDatabase, ORID iRecordId) {
		super(iDatabase);
		recordId = (ORecordId) iRecordId;
		setup();
	}

	public void reset(byte[] iSource) {
		reset();
		source = iSource;
	}

	public ORecordBytes copy() {
		ORecordBytes cloned = new ORecordBytes();
		cloned.source = source;
		cloned.database = database;
		cloned.recordId = recordId.copy();
		return cloned;
	}

	@Override
	public byte[] toStream() {
		return source;
	}
}
