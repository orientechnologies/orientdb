package com.orientechnologies.orient.kv.index;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * Wrapper class for persistent tree map. It handles the asynchronous commit of changes done by the external
 * OTreeMapPersistentAsynchThread singleton thread.
 * 
 * @author luca
 * 
 * @param <K>
 * @param <V>
 * @see OTreeMapPersistentAsynchThread
 */
@SuppressWarnings("serial")
public class OTreeMapPersistentAsynch<K, V> extends OTreeMapPersistent<K, V> {

	public OTreeMapPersistentAsynch(ODatabaseRecord<?> iDatabase, String iClusterName, OStreamSerializer iKeySerializer,
			OStreamSerializer iValueSerializer) {
		super(iDatabase, iClusterName, iKeySerializer, iValueSerializer);
		OTreeMapPersistentAsynchThread.getInstance().registerMap(this);
	}

	public OTreeMapPersistentAsynch(ODatabaseRecord<?> iDatabase, String iClusterName, ORID iRID) {
		super(iDatabase, iClusterName, iRID);
		OTreeMapPersistentAsynchThread.getInstance().registerMap(this);
	}

	/**
	 * Don't commit changes since they are scheduled by the external OTreeMapPersistentAsynchThread singleton thread.
	 * 
	 * @see OTreeMapPersistentAsynchThread#execute()
	 */
	@Override
	public void commitChanges() {
	}

	/**
	 * Commit changes for real. It's called by OTreeMapPersistentAsynchThread singleton thread.
	 * 
	 * @see OTreeMapPersistentAsynchThread#execute()
	 */
	public void executeCommitChanges() {
		super.commitChanges();
	}
}
