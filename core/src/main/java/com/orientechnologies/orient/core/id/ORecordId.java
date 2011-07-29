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
package com.orientechnologies.orient.core.id;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class ORecordId implements ORID {
	private static final long			serialVersionUID				= 247070594054408657L;

	public static final int				PERSISTENT_SIZE					= OConstants.SIZE_SHORT + OConstants.SIZE_LONG;

	public static final ORecordId	EMPTY_RECORD_ID					= new ORecordId();
	public static final byte[]		EMPTY_RECORD_ID_STREAM	= EMPTY_RECORD_ID.toStream();

	public int										clusterId								= CLUSTER_ID_INVALID;
	public long										clusterPosition					= CLUSTER_POS_INVALID;

	public ORecordId() {
	}

	public ORecordId(final int iClusterId, final long iPosition) {
		clusterId = iClusterId;
		checkClusterLimits();
		clusterPosition = iPosition;
	}

	public ORecordId(final int iClusterIdId) {
		clusterId = iClusterIdId;
		checkClusterLimits();
	}

	public ORecordId(final String iRecordId) {
		fromString(iRecordId);
	}

	/**
	 * Copy constructor.
	 * 
	 * @param parentRid
	 *          Source object
	 */
	public ORecordId(final ORID parentRid) {
		clusterId = parentRid.getClusterId();
		clusterPosition = parentRid.getClusterPosition();
	}

	public void reset() {
		clusterId = CLUSTER_ID_INVALID;
		clusterPosition = CLUSTER_POS_INVALID;
	}

	public boolean isValid() {
		return clusterPosition != CLUSTER_POS_INVALID;
	}

	public boolean isPersistent() {
		return clusterPosition > -1;
	}

	public boolean isNew() {
		return clusterPosition < 0;
	}

	public boolean isTemporary() {
		return clusterId == -1 && clusterPosition < -1;
	}

	@Override
	public String toString() {
		return generateString(clusterId, clusterPosition);
	}

	public StringBuilder toString(StringBuilder iBuffer) {
		if (iBuffer == null)
			iBuffer = new StringBuilder();

		iBuffer.append(PREFIX);
		iBuffer.append(clusterId);
		iBuffer.append(SEPARATOR);
		iBuffer.append(clusterPosition);
		return iBuffer;
	}

	public static String generateString(final int iClusterId, final long iPosition) {
		final StringBuilder buffer = new StringBuilder(12);
		buffer.append(PREFIX);
		buffer.append(iClusterId);
		buffer.append(SEPARATOR);
		buffer.append(iPosition);
		return buffer.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + clusterId;
		result = prime * result + (int) (clusterPosition ^ (clusterPosition >>> 32));
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof ORecordId))
			return false;
		final ORecordId other = (ORecordId) obj;
		if (clusterId != other.clusterId)
			return false;
		if (clusterPosition != other.clusterPosition)
			return false;
		return true;
	}

	public int compareTo(final OIdentifiable iOther) {
		if (iOther == this)
			return 0;

		if (iOther == null)
			return 1;

		final ORID other = iOther.getIdentity();

		if (clusterId == other.getClusterId()) {
			if (clusterPosition == other.getClusterPosition())
				return 0;
			else if (clusterPosition > other.getClusterPosition())
				return 1;
			else if (clusterPosition < other.getClusterPosition())
				return -1;
		} else if (clusterId > other.getClusterId())
			return 1;

		return -1;
	}

	public int compare(final OIdentifiable iObj1, final OIdentifiable iObj2) {
		if (iObj1 == iObj2)
			return 0;

		if (iObj1 != null)
			return iObj1.compareTo(iObj2);

		return -1;
	}

	public ORecordId copy() {
		return new ORecordId(clusterId, clusterPosition);
	}

	private void checkClusterLimits() {
		if (clusterId < -1)
			throw new ODatabaseException("RecordId can't support negative cluster id. You've used: " + clusterId);

		if (clusterId > CLUSTER_MAX)
			throw new ODatabaseException("RecordId can't support cluster id major than 32767. You've used: " + clusterId);
	}

	public ORecordId fromStream(final InputStream iStream) throws IOException {
		clusterId = OBinaryProtocol.bytes2short(iStream);
		clusterPosition = OBinaryProtocol.bytes2long(iStream);
		return this;
	}

	public ORecordId fromStream(final byte[] iBuffer) {
		clusterId = OBinaryProtocol.bytes2short(iBuffer, 0);
		clusterPosition = OBinaryProtocol.bytes2long(iBuffer, OConstants.SIZE_SHORT);
		return this;
	}

	public int toStream(final OutputStream iStream) throws IOException {
		final int beginOffset = OBinaryProtocol.short2bytes((short) clusterId, iStream);
		OBinaryProtocol.long2bytes(clusterPosition, iStream);
		return beginOffset;
	}

	public byte[] toStream() {
		byte[] buffer = new byte[PERSISTENT_SIZE];
		OBinaryProtocol.short2bytes((short) clusterId, buffer, 0);
		OBinaryProtocol.long2bytes(clusterPosition, buffer, OConstants.SIZE_SHORT);
		return buffer;
	}

	public int getClusterId() {
		return clusterId;
	}

	public long getClusterPosition() {
		return clusterPosition;
	}

	public void fromString(final String iRecordId) {
		if (iRecordId == null) {
			clusterId = CLUSTER_ID_INVALID;
			clusterPosition = CLUSTER_POS_INVALID;
			return;
		}

		if (!OStringSerializerHelper.contains(iRecordId, SEPARATOR))
			throw new IllegalArgumentException("Argument '" + iRecordId
					+ "' is not a RecordId in form of string. Format must be: <cluster-id>:<cluster-position>");

		final List<String> parts = OStringSerializerHelper.split(iRecordId, SEPARATOR, PREFIX);

		if (parts.size() != 2)
			throw new IllegalArgumentException("Argument received '" + iRecordId
					+ "' is not a RecordId in form of string. Format must be: #<cluster-id>:<cluster-position>. Example: #3:12");

		clusterId = Integer.parseInt(parts.get(0));
		checkClusterLimits();
		clusterPosition = Long.parseLong(parts.get(1));
	}

	public void copyFrom(final ORID iSource) {
		if (iSource == null)
			throw new IllegalArgumentException("Source is null");

		clusterId = iSource.getClusterId();
		clusterPosition = iSource.getClusterPosition();
	}

	public String next() {
		return generateString(clusterId, clusterPosition + 1);
	}

	public ORID getIdentity() {
		return this;
	}

	public ORecord<?> getRecord() {
		return ODatabaseRecordThreadLocal.INSTANCE.get().load(this);
	}
}
