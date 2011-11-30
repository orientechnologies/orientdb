/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.type.tree.provider;

import java.io.IOException;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * Handles a set of references optimizing the memory space used. This is used by LINKSET type and as SET of links of Not Unique
 * indexes. The binary chunk is allocated the first time and never changes. This takes more memory but assure zero fragmentation at
 * the storage level. <br>
 * Structure of binary chunk:<br>
 * <code>
 * +-----------+--------+------------+----------+-----------+---------------------+<br>
 * | NODE SIZE | COLOR .| PARENT RID | LEFT RID | RIGHT RID | RID LIST .......... |<br>
 * +-----------+--------+------------+----------+-----------+---------------------+<br>
 * | 4 bytes . | 1 byte | 10 bytes ..| 10 bytes | 10 bytes .| 10 * MAX_SIZE bytes |<br>
 * +-----------+--------+------------+----------+-----------+---------------------+<br>
 * = 35 bytes + 10 * MAX_SIZE bytes
 * </code>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com) *
 * 
 */
public class OMVRBTreeRIDEntryProvider extends OMVRBTreeEntryDataProviderAbstract<ORecordId, ORecordId> {
	private static final long		serialVersionUID	= 1L;

	protected final static int	OFFSET_NODESIZE		= 0;
	protected final static int	OFFSET_COLOR			= OFFSET_NODESIZE + OBinaryProtocol.SIZE_INT;
	protected final static int	OFFSET_PARENT			= OFFSET_COLOR + OBinaryProtocol.SIZE_BYTE;
	protected final static int	OFFSET_LEFT				= OFFSET_PARENT + ORecordId.PERSISTENT_SIZE;
	protected final static int	OFFSET_RIGHT			= OFFSET_LEFT + ORecordId.PERSISTENT_SIZE;
	protected final static int	OFFSET_RIDLIST		= OFFSET_RIGHT + ORecordId.PERSISTENT_SIZE;

	public OMVRBTreeRIDEntryProvider(final OMVRBTreeRIDProvider iTreeDataProvider) {
		super(iTreeDataProvider);
	}

	public OMVRBTreeRIDEntryProvider(final OMVRBTreeRIDProvider iTreeDataProvider, final ORID iRID) {
		super(iTreeDataProvider, iRID);
	}

	public ORecordId getKeyAt(final int iIndex) {
		return new ORecordId().fromStream(moveToIndex(iIndex));
	}

	public ORecordId getValueAt(final int iIndex) {
		throw new UnsupportedOperationException("getValueAt");
	}

	public boolean setValueAt(int iIndex, final ORecordId iValue) {
		throw new UnsupportedOperationException("setValueAt");
	}

	public boolean insertAt(final int iIndex, final ORecordId iKey, final ORecordId iValue) {
		if (iIndex < size)
			// MOVE RIGHT TO MAKE ROOM FOR THE ITEM
			stream.move(getKeyPositionInStream(iIndex), getKeyPositionInStream(iIndex + 1));

		try {
			iKey.toStream(moveToIndex(iIndex));
		} catch (IOException e) {
			throw new OSerializationException("Cannot serialize entryRID object: " + this, e);
		}

		size++;

		return setDirty();
	}

	public boolean removeAt(final int iIndex) {
		if (iIndex > -1 && iIndex < size - 1)
			// SHIFT LEFT THE VALUES
			stream.move(getKeyPositionInStream(iIndex + 1), getKeyPositionInStream(iIndex));

		// FREE RESOURCES
		size--;
		return setDirty();
	}

	public boolean copyDataFrom(final OMVRBTreeEntryDataProvider<ORecordId, ORecordId> iFrom, int iStartPosition) {
		OMVRBTreeRIDEntryProvider parent = (OMVRBTreeRIDEntryProvider) iFrom;
		size = iFrom.getSize() - iStartPosition;
		stream.jump(0).copyFrom(parent.moveToIndex(iStartPosition), size);
		stream.setSource(parent.stream.copy());
		return setDirty();
	}

	public boolean truncate(final int iNewSize) {
		stream.jump(iNewSize).fill(size - iNewSize, (byte) 0);
		size = iNewSize;
		return setDirty();
	}

	public boolean copyFrom(final OMVRBTreeEntryDataProvider<ORecordId, ORecordId> iSource) {
		final OMVRBTreeRIDEntryProvider source = (OMVRBTreeRIDEntryProvider) iSource;

		stream = source.stream;
		size = source.size;

		return setDirty();
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		stream.setSource(iStream);

		size = stream.jump(OFFSET_NODESIZE).getAsInteger();
		color = stream.jump(OFFSET_COLOR).getAsBoolean();
		parentRid.fromStream(stream.jump(OFFSET_PARENT));
		leftRid.fromStream(stream.jump(OFFSET_LEFT));
		rightRid.fromStream(stream.jump(OFFSET_RIGHT));

		return this;
	}

	public byte[] toStream() throws OSerializationException {
		try {
			stream.jump(OFFSET_NODESIZE).set(size);
			stream.jump(OFFSET_COLOR).set(color);
			parentRid.toStream(stream.jump(OFFSET_PARENT));
			leftRid.toStream(stream.jump(OFFSET_LEFT));
			rightRid.toStream(stream.jump(OFFSET_RIGHT));
		} catch (IOException e) {
			throw new OSerializationException("Cannot serialize tree entry RID node: " + this, e);
		}

		// RETURN DIRECTLY THE UNDERLYING BUFFER SINCE IT'S FIXED
		final byte[] buffer = stream.toByteArray();
		record.fromStream(buffer);
		return buffer;
	}

	protected OMemoryStream moveToIndex(final int iIndex) {
		return stream.jump(getKeyPositionInStream(iIndex));
	}

	protected int getKeyPositionInStream(final int iIndex) {
		return OFFSET_RIDLIST + (iIndex * ORecordId.PERSISTENT_SIZE);
	}
}
