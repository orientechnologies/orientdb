package com.orientechnologies.orient.core.type.tree.generic;

import java.io.IOException;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.OTreeDataProvider;
import com.orientechnologies.orient.core.type.tree.OTreeEntryDataProvider;

public class OTreeDataProviderGeneric<K, V> implements OTreeDataProvider<K, V>, OSerializableStream {
	public final static byte						CURRENT_PROTOCOL_VERSION	= 0;

	protected int												size;
	protected int												defaultPageSize;
	protected OStreamSerializer					keySerializer;
	protected OStreamSerializer					valueSerializer;
	protected final String							clusterName;
	protected final int									clusterId;
	protected ORecordId									root;
	protected ORecordBytesLazy					record;
	protected final OMemoryOutputStream	entryRecordBuffer;
	protected boolean										keepKeysInMemory;
	protected boolean										keepValuesInMemory;

	protected OStorage									storage;

	public OTreeDataProviderGeneric(final OStorage iStorage, final String iClusterName, final ORID iRID) {
		this(iStorage, iClusterName, null, null);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OTreeDataProviderGeneric(final OStorage iStorage, final String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {

		storage = iStorage;
		clusterName = iClusterName;
		if (storage != null) {
			if (clusterName != null)
				clusterId = storage.getClusterIdByName(iClusterName);
			else
				clusterId = storage.getClusterIdByName(OStorage.CLUSTER_INDEX_NAME);
		} else {
			// CLUSTER ID NOT USED FOR DATABASE INDEX
			clusterId = -1;
		}

		record = new ORecordBytesLazy(this);
		record.setIdentity(new ORecordId());

		keySerializer = iKeySerializer;
		valueSerializer = iValueSerializer;

		entryRecordBuffer = new OMemoryOutputStream(getDefaultPageSize() * 15);
		updateConfig();
	}

	public int getSize() {
		return size;
	}

	public int getDefaultPageSize() {
		return defaultPageSize;
	}

	public ORID getRoot() {
		return root;
	}

	public boolean setSize(final int iSize) {
		size = iSize;
		return setDirty();
	}

	public boolean setRoot(final ORID iRid) {
		if (root == null)
			root = new ORecordId();

		if (iRid == null)
			root.reset();
		else if (!iRid.equals(root))
			root.copyFrom(iRid);
		
		return setDirty();
	}

	public boolean isTreeDirty() {
		return record.isDirty();
	}

	public OTreeEntryDataProvider<K, V> getEntry(final ORID iRid) {
		return new OTreeEntryDataProviderGeneric<K, V>(this, iRid);
	}

	public OTreeEntryDataProvider<K, V> createEntry() {
		return new OTreeEntryDataProviderGeneric<K, V>(this);
	}

	public boolean updateConfig() {
		boolean isChanged = false;

		int newSize = OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.getValueAsInteger();
		if (newSize != defaultPageSize) {
			defaultPageSize = newSize;
			isChanged = true;
		}

		keepKeysInMemory = OGlobalConfiguration.MVRBTREE_ENTRY_KEYS_IN_MEMORY.getValueAsBoolean();
		keepValuesInMemory = OGlobalConfiguration.MVRBTREE_ENTRY_VALUES_IN_MEMORY.getValueAsBoolean();

		return isChanged ? setDirty() : false;
	}

	public void load() {
		if (storage == null)
			load(getDatabase());
		else
			load(storage);
	}

	protected void load(final ODatabaseRecord iDb) {
		if (!record.getIdentity().isValid())
			return;
		record.setDatabase(iDb);
		record.reload();
		record.recycle(this);
		fromStream(record.toStream());
	}

	protected void load(final OStorage iSt) {
		if (!record.getIdentity().isValid())
			// NOTHING TO LOAD
			return;
		ORawBuffer raw = iSt.readRecord(null, (ORecordId) record.getIdentity(), null, null);
		if (raw == null)
			throw new OConfigurationException("Cannot load map with id " + record.getIdentity());
		record.setVersion(raw.version);
		record.recycle(this);
		fromStream(raw.buffer);
	}

	protected void save(final ODatabaseRecord iDb) {
		record.fromStream(toStream());
		record.setDatabase(iDb);
		record.save(clusterName);
	}

	public void save() {
		if (storage == null)
			save(getDatabase());
		else
			save(storage);
	}

	protected void save(final OStorage iSt) {
		record.fromStream(toStream());
		if (record.getIdentity().isValid())
			// UPDATE IT WITHOUT VERSION CHECK SINCE ALL IT'S LOCKED
			record.setVersion(iSt.updateRecord((ORecordId) record.getIdentity(), record.toStream(), -1, record.getRecordType(), null));
		else {
			// CREATE IT
			if (record.getIdentity().getClusterId() == ORID.CLUSTER_ID_INVALID)
				((ORecordId) record.getIdentity()).clusterId = clusterId;

			iSt.createRecord((ORecordId) record.getIdentity(), record.toStream(), record.getRecordType(), null);
		}
		record.unsetDirty();
	}

	public void delete() {
		if (storage == null)
			delete(getDatabase());
		else
			delete(storage);
		root = null;
	}

	protected void delete(final ODatabaseRecord iDb) {
		iDb.delete(record);
	}

	protected void delete(final OStorage iSt) {
		iSt.deleteRecord((ORecordId) record.getIdentity(), record.getVersion(), null);
	}

	public byte[] toStream() throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		// XXX Sylvain : really necessary ?
		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		final Integer identityRecord = System.identityHashCode(record);
		final Set<Integer> marshalledRecords = OSerializationThreadLocal.INSTANCE.get();
		if (marshalledRecords.contains(identityRecord)) {
			// ALREADY IN STACK, RETURN EMPTY
			return new byte[] {};
		} else
			marshalledRecords.add(identityRecord);

		try {
			OMemoryOutputStream stream = entryRecordBuffer;
			stream.add(CURRENT_PROTOCOL_VERSION);
			stream.addAsFixed(root != null ? root.toStream() : ORecordId.EMPTY_RECORD_ID_STREAM);

			stream.add(size);
			stream.add(defaultPageSize);

			stream.add(keySerializer.getName());
			stream.add(valueSerializer.getName());

			byte[] result = stream.getByteArray();
			record.fromStream(result);
			return result;

		} catch (IOException e) {
			throw new OSerializationException("Error on marshalling RB+Tree", e);
		} finally {
			marshalledRecords.remove(identityRecord);
			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.toStream", timer);
		}
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();
		try {
			final OMemoryStream stream = new OMemoryStream(iStream);
			byte protocolVersion = stream.peek();
			if (protocolVersion != -1) {
				// @COMPATIBILITY BEFORE 0.9.25
				stream.getAsByte();
				if (protocolVersion != CURRENT_PROTOCOL_VERSION)
					throw new OSerializationException(
							"The index has been created with a previous version of OrientDB. Soft transitions between versions is supported since 0.9.25. To use it with this version of OrientDB you need to export and import your database. "
									+ protocolVersion + "<->" + CURRENT_PROTOCOL_VERSION);
			}

			root = new ORecordId();
			root.fromStream(stream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

			size = stream.getAsInteger();
			if (protocolVersion == -1)
				// @COMPATIBILITY BEFORE 0.9.25
				defaultPageSize = stream.getAsShort();
			else
				defaultPageSize = stream.getAsInteger();

			serializerFromStream(stream);

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on unmarshalling OMVRBTreePersistent object from record: %s", e,
					OSerializationException.class, root);
		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.fromStream", timer);
		}
		return this;
	}

	protected void serializerFromStream(final OMemoryStream stream) throws IOException {
		keySerializer = OStreamSerializerFactory.get(stream.getAsString());
		valueSerializer = OStreamSerializerFactory.get(stream.getAsString());
	}

	public String toString() {
		return "index " + record.getIdentity();
	}

	@Override
	public int hashCode() {
		final ORID rid = record.getIdentity();
		return rid == null ? 0 : rid.hashCode();
	}

	public ORecordBytesLazy getRecord() {
		return record;
	}

	protected ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	protected boolean setDirty() {
		if (record.isDirty())
			return false;
		record.setDirty();
		return true;
	}
}
