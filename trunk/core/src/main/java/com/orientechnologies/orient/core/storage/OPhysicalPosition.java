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

import java.io.IOException;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

public class OPhysicalPosition implements OSerializableStream {
	public int	dataSegment;	// ID OF DATA SEGMENT
	public long	dataPosition; // OFFSET IN BYTES INSIDE THE DATA SEGMENT
	public int	recordSize;	// SIZE IN BYTES OF THE RECORD
	public int	version	= 0;	// RECORD VERSION

	public OPhysicalPosition() {
	}

	public OPhysicalPosition(final int iDataSegment, final long iPosition) {
		dataSegment = iDataSegment;
		dataPosition = iPosition;
	}

	public void copyTo(final OPhysicalPosition iDest) {
		iDest.dataSegment = dataSegment;
		iDest.dataPosition = dataPosition;
		iDest.recordSize = recordSize;
		iDest.version = version;
	}

	public void copyFrom(final OPhysicalPosition iSource) {
		iSource.copyTo(this);
	}

	@Override
	public String toString() {
		return "dataSegment=" + dataSegment + ", recordPosition=" + dataPosition + ", recordSize=" + recordSize + ", v=" + version;
	}

	public OSerializableStream fromStream(byte[] iStream) throws IOException {
		int pos = 0;

		dataSegment = OBinaryProtocol.bytes2int(iStream, pos);
		pos += OConstants.SIZE_INT;

		dataPosition = OBinaryProtocol.bytes2long(iStream, pos);
		pos += OConstants.SIZE_LONG;

		recordSize = OBinaryProtocol.bytes2int(iStream, pos);
		pos += OConstants.SIZE_INT;

		version = OBinaryProtocol.bytes2int(iStream, pos);

		return this;
	}

	public byte[] toStream() throws IOException {
		byte[] buffer = new byte[OConstants.SIZE_INT + OConstants.SIZE_LONG + OConstants.SIZE_INT + OConstants.SIZE_INT];
		int pos = 0;

		OBinaryProtocol.int2bytes(dataSegment, buffer, pos);
		pos += OConstants.SIZE_INT;

		OBinaryProtocol.long2bytes(dataPosition, buffer, pos);
		pos += OConstants.SIZE_LONG;

		OBinaryProtocol.int2bytes(recordSize, buffer, pos);
		pos += OConstants.SIZE_INT;

		OBinaryProtocol.int2bytes(version, buffer, pos);
		return buffer;
	}
}
