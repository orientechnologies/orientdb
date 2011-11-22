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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

@SuppressWarnings("serial")
public class OPhysicalPosition implements OSerializableStream, Comparable<OPhysicalPosition> {
	public int	dataSegment;	// ID OF DATA SEGMENT
	public long	dataPosition; // OFFSET IN BYTES INSIDE THE DATA SEGMENT
	public byte	type;				// RECORD TYPE
	public int	version	= 0;	// RECORD VERSION

	public int	recordSize;	// SIZE IN BYTES OF THE RECORD. USED ONLY IN MEMORY

	public OPhysicalPosition() {
	}

	public OPhysicalPosition(final int iDataSegment, final long iPosition, final byte iRecordType) {
		dataSegment = iDataSegment;
		dataPosition = iPosition;
		type = iRecordType;
	}

	public void copyTo(final OPhysicalPosition iDest) {
		iDest.dataSegment = dataSegment;
		iDest.dataPosition = dataPosition;
		iDest.type = type;
		iDest.version = version;
		iDest.recordSize = recordSize;
	}

	public void copyFrom(final OPhysicalPosition iSource) {
		iSource.copyTo(this);
	}

	@Override
	public String toString() {
		return "dataSegment=" + dataSegment + ", recordPosition=" + dataPosition + ", type=" + type + ", recordSize=" + recordSize
				+ ", v=" + version;
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		int pos = 0;

		dataSegment = OBinaryProtocol.bytes2int(iStream, pos);
		pos += OBinaryProtocol.SIZE_INT;

		dataPosition = OBinaryProtocol.bytes2long(iStream, pos);
		pos += OBinaryProtocol.SIZE_LONG;

		type = iStream[pos];
		pos += OBinaryProtocol.SIZE_BYTE;

		recordSize = OBinaryProtocol.bytes2int(iStream, pos);
		pos += OBinaryProtocol.SIZE_INT;

		version = OBinaryProtocol.bytes2int(iStream, pos);

		return this;
	}

	public byte[] toStream() throws OSerializationException {
		byte[] buffer = new byte[OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_LONG + OBinaryProtocol.SIZE_BYTE + OBinaryProtocol.SIZE_INT
				+ OBinaryProtocol.SIZE_INT];
		int pos = 0;

		OBinaryProtocol.int2bytes(dataSegment, buffer, pos);
		pos += OBinaryProtocol.SIZE_INT;

		OBinaryProtocol.long2bytes(dataPosition, buffer, pos);
		pos += OBinaryProtocol.SIZE_LONG;

		buffer[pos] = type;
		pos += OBinaryProtocol.SIZE_BYTE;

		OBinaryProtocol.int2bytes(recordSize, buffer, pos);
		pos += OBinaryProtocol.SIZE_INT;

		OBinaryProtocol.int2bytes(version, buffer, pos);
		return buffer;
	}

	public int compareTo(final OPhysicalPosition iOther) {
		return (int) (dataPosition - iOther.dataPosition);
	}
}
