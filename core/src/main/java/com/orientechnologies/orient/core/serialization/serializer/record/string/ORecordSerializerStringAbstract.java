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
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.util.Map;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;

public abstract class ORecordSerializerStringAbstract implements ORecordSerializer {
	protected abstract String toString(final ORecordInternal<?> iRecord, final OUserObject2RecordHandler iObjHandler,
			final Map<ORecordInternal<?>, ORecordId> iMarshalledRecords);

	protected abstract ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, final String iContent,
			final ORecordInternal<?> iRecord);

	public String toString(ORecordInternal<?> iRecord) {
		return toString(iRecord, iRecord.getDatabase(), OSerializationThreadLocal.INSTANCE.get());
	}

	public ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, final String iSource) {
		return fromString(iDatabase, iSource, iDatabase.newInstance());
	}

	public ORecordInternal<?> fromStream(final ODatabaseRecord<?> iDatabase, final byte[] iSource, final ORecordInternal<?> iRecord) {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			return fromString(iDatabase, OBinaryProtocol.bytes2string(iSource), iRecord);
		} finally {

			OProfiler.getInstance().stopChrono("ORecordSerializerStringAbstract.fromStream", timer);
		}
	}

	public byte[] toStream(final ODatabaseRecord<?> iDatabase, final ORecordInternal<?> iRecord) {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			return OBinaryProtocol.string2bytes(toString((ORecordSchemaAware<?>) iRecord, iDatabase, OSerializationThreadLocal.INSTANCE
					.get()));
		} finally {

			OProfiler.getInstance().stopChrono("ORecordSerializerStringAbstract.toStream", timer);
		}
	}
}
