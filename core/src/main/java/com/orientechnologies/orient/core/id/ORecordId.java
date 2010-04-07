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

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

public class ORecordId implements ORID {
	public static final char			BEGIN_CHAR							= '#';
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
		if (!iRecordId.contains(SEPARATOR))
			throw new IllegalArgumentException("Argument is not a RecordId in form of string");

		final String parts[] = iRecordId.split(SEPARATOR);
		clusterId = Integer.parseInt(parts[0]);
		checkClusterLimits();
		clusterPosition = Long.parseLong(parts[1]);
	}

	public void reset() {
		clusterId = CLUSTER_ID_INVALID;
		clusterPosition = CLUSTER_POS_INVALID;
	}

	public boolean isValid() {
		return clusterId != CLUSTER_ID_INVALID && clusterPosition != CLUSTER_POS_INVALID;
	}

	@Override
	public String toString() {
		return generateString(clusterId, clusterPosition);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		long result = 1;
		result = prime * result + clusterId;
		result = prime * result + clusterPosition;
		return (int) result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final ORecordId other = (ORecordId) obj;
		if (clusterId != other.clusterId)
			return false;
		if (clusterPosition != other.clusterPosition)
			return false;
		return true;
	}

	public ORecordId copy() {
		return new ORecordId(clusterId, clusterPosition);
	}

	private void checkClusterLimits() {
		if (clusterId > CLUSTER_MAX)
			throw new ODatabaseException("RecordId can't support cluster id major than 32767. You've used: " + clusterId);
	}

	public static String generateString(final int iClusterId, final long iPosition) {
		return iClusterId + SEPARATOR + iPosition;
	}

	public ORecordId fromStream(final byte[] iBuffer) {
		clusterId = OBinaryProtocol.bytes2short(iBuffer, 0);
		clusterPosition = OBinaryProtocol.bytes2long(iBuffer, OConstants.SIZE_SHORT);
		return this;
	}

	public byte[] toStream() {
		byte[] buffer = new byte[PERSISTENT_SIZE];
		OBinaryProtocol.short2bytes((short) clusterId, buffer, 0);
		OBinaryProtocol.long2bytes((short) clusterPosition, buffer, OConstants.SIZE_SHORT);
		return buffer;
	}

	public int getClusterId() {
		return clusterId;
	}

	public long getClusterPosition() {
		return clusterPosition;
	}
}
