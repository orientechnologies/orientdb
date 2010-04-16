package com.orientechnologies.orient.enterprise.distributed.hazelcast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

@SuppressWarnings("serial")
public class ODistributedRecordId implements DataSerializable {
	public String			dbName;
	public ORecordId	rid;
	public int				requesterId;
	public int				version;

	/**
	 * Constructor used by Serialization.
	 */
	public ODistributedRecordId() {
	}

	public ODistributedRecordId(final String iDbName, final ORID iRID, final int iVersion) {
		dbName = iDbName;
		rid = (ORecordId) iRID;
		version = iVersion;
	}

	public ODistributedRecordId(final String iDbName) {
		dbName = iDbName;
	}

	public ODistributedRecordId(final String iDbName, final int iRequesterId, final ORecordId iRecordId, final int iVersion) {
		this(iDbName, iRecordId, iVersion);
		requesterId = iRequesterId;
	}

	@Override
	public String toString() {
		return dbName + "@" + rid.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = rid.hashCode();
		result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!rid.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;

		ODistributedRecordId other = (ODistributedRecordId) obj;
		if (dbName == null) {
			if (other.dbName != null)
				return false;
		} else if (!dbName.equals(other.dbName))
			return false;
		return true;
	}

	public void readData(final DataInput iInput) throws IOException {
		dbName = iInput.readUTF();
		if (rid == null)
			rid = new ORecordId();
		rid.clusterId = iInput.readShort();
		rid.clusterPosition = iInput.readLong();
		requesterId = iInput.readInt();
		version = iInput.readInt();
	}

	public void writeData(final DataOutput iOutput) throws IOException {
		iOutput.writeUTF(dbName);
		iOutput.writeShort(rid != null ? rid.clusterId : -1);
		iOutput.writeLong(rid != null ? rid.clusterPosition : -1);
		iOutput.writeInt(requesterId);
		iOutput.writeInt(version);
	}
}
