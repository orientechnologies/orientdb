package com.orientechnologies.orient.client.distributed;

import java.io.IOException;
import java.util.Map;

import com.hazelcast.impl.base.AddressAwareException;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.enterprise.distributed.ODistributedException;
import com.orientechnologies.orient.enterprise.distributed.hazelcast.ODistributedRecordId;

public abstract class OStorageDistributed extends OStorageRemote {

	public OStorageDistributed(String iName, String iMode) throws IOException {
		super(iName, iMode);
	}

	protected abstract void checkOpeness();

	protected abstract Map<Object, Object> getDistributedDatabaseMap();

	public long createRecord(final int iClusterId, final byte[] iContent, final byte iRecordType) {
		final ODistributedRecordId key = new ODistributedRecordId(name, iClusterId);

		getDistributedDatabaseMap().put(key, new ORawBuffer(iContent, 0, iRecordType));

		return ((ORawBuffer) getDistributedDatabaseMap().get(key)).newPosition;
	}

	public ORawBuffer readRecord(final int iRequesterId, int iClusterId, long iPosition) {
		Object result = getDistributedDatabaseMap().get(
				new ODistributedRecordId(name, iRequesterId, new ORecordId(iClusterId, iPosition), -1));

		if (result instanceof ORawBuffer)
			return (ORawBuffer) result;
		else if (result instanceof AddressAwareException)
			throw new ODistributedException("Error on reading record from the cluster: " + ((AddressAwareException) result).getAddress(),
					((AddressAwareException) result).getException());

		return null;
	}

	public int updateRecord(final int iRequesterId, int iClusterId, long iPosition, byte[] iContent, int iVersion, byte iRecordType) {
		ODistributedRecordId key = new ODistributedRecordId(name, iRequesterId, new ORecordId(iClusterId, iPosition), iVersion);

		getDistributedDatabaseMap().put(key, new ORawBuffer(iContent, 0, iRecordType));

		return ((ORawBuffer) getDistributedDatabaseMap().get(key)).version;
	}

	public void deleteRecord(final int iRequesterId, final int iClusterId, final long iPosition, final int iVersion) {
		getDistributedDatabaseMap()
				.remove(new ODistributedRecordId(name, iRequesterId, new ORecordId(iClusterId, iPosition), iVersion));
	}

	public void deleteRecord(final int iRequesterId, final ORID iRecordId, final int iVersion) {
		getDistributedDatabaseMap().remove(new ODistributedRecordId(name, iRecordId, iVersion));
	}
}
