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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * Contains the information about a database operation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordOperation implements OSerializableStream {

	private static final long	serialVersionUID	= 1L;

	public static final byte	LOADED						= 0;
	public static final byte	UPDATED						= 1;
	public static final byte	DELETED						= 2;
	public static final byte	CREATED						= 3;

	public long								serial;
	public byte								type;
	public OIdentifiable			record;

	public int								dataSegmentId			= 0;	// DEFAULT ONE

	public ORecordOperation() {
	}

	public ORecordOperation(final OIdentifiable iRecord, final byte iStatus) {
		// CLONE RECORD AND CONTENT
		this.record = iRecord;
		this.type = iStatus;
	}

	@Override
	public int hashCode() {
		return record.getIdentity().hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof ORecordOperation))
			return false;

		return record.equals(((ORecordOperation) obj).record);
	}

	@Override
	public String toString() {
		return new StringBuilder().append("ORecordOperation [record=").append(record).append(", type=").append(getName(type))
				.append("]").toString();
	}

	public ORecordInternal<?> getRecord() {
		return (ORecordInternal<?>) (record != null ? record.getRecord() : null);
	}

	public byte[] toStream() throws OSerializationException {
		try {
			final OMemoryStream stream = new OMemoryStream();
			stream.set(serial);
			stream.set(type);
			((ORecordId) record.getIdentity()).toStream(stream);

			switch (type) {
			case CREATED:
			case UPDATED:
				stream.set(((ORecordInternal<?>) record.getRecord()).getRecordType());
				stream.set(((ORecordInternal<?>) record.getRecord()).toStream());
				break;
			}

			return stream.toByteArray();

		} catch (Exception e) {
			throw new OSerializationException("Cannot serialize record operation", e);
		}
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		try {
			final OMemoryStream stream = new OMemoryStream(iStream);
			serial = stream.getAsLong();
			type = stream.getAsByte();
			final ORecordId rid = new ORecordId().fromStream(stream);

			switch (type) {
			case CREATED:
			case UPDATED:
				record = Orient.instance().getRecordFactoryManager().newInstance(stream.getAsByte());
				((ORecordInternal<?>) record).fill(rid, 0, stream.getAsByteArray(), true);
				break;
			}

			return this;

		} catch (Exception e) {
			throw new OSerializationException("Cannot deserialize record operation", e);
		}
	}

	public static String getName(final int type) {
		String operation = "?";
		switch (type) {
		case ORecordOperation.CREATED:
			operation = "CREATE";
			break;
		case ORecordOperation.UPDATED:
			operation = "UPDATE";
			break;
		case ORecordOperation.DELETED:
			operation = "DELETE";
			break;
		}
		return operation;
	}
}
