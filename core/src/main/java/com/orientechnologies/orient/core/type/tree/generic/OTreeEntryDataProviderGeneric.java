package com.orientechnologies.orient.core.type.tree.generic;

import java.io.IOException;
import java.util.Arrays;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.OTreeEntryDataProvider;

public class OTreeEntryDataProviderGeneric<K, V> implements OTreeEntryDataProvider<K, V>, OSerializableStream {

	protected OTreeDataProviderGeneric<K, V>	treeDataProvider;

	protected int															size			= 0;
	protected int															pageSize;
	protected K[]															keys;
	protected V[]															values;
	protected int[]														serializedKeys;
	protected int[]														serializedValues;
	protected ORecordId												parentRid;
	protected ORecordId												leftRid;
	protected ORecordId												rightRid;

	protected boolean													color			= OMVRBTree.RED;

	protected ORecordBytesLazy								record;

	protected OMemoryInputStream							inStream	= new OMemoryInputStream();

	public OTreeEntryDataProviderGeneric(final OTreeDataProviderGeneric<K, V> iTreeDataProvider) {
		treeDataProvider = iTreeDataProvider;
		record = new ORecordBytesLazy(this);
		record.setIdentity(new ORecordId());
		pageSize = treeDataProvider.getDefaultPageSize();
		parentRid = new ORecordId();
		leftRid = new ORecordId();
		rightRid = new ORecordId();
		keys = (K[]) new Object[pageSize];
		values = (V[]) new Object[pageSize];
		serializedKeys = new int[pageSize];
		serializedValues = new int[pageSize];
	}

	public OTreeEntryDataProviderGeneric(final OTreeDataProviderGeneric<K, V> iTreeDataProvider, final ORID iRID) {
		super();
		treeDataProvider = iTreeDataProvider;
		record = new ORecordBytesLazy(this);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
		if (treeDataProvider.storage == null)
			load(treeDataProvider.getDatabase());
		else
			load(treeDataProvider.storage);
	}

	protected void load(final ODatabaseRecord iDb) {
		try {
			record.setDatabase(iDb);
			record.reload();
		} catch (Exception e) {
			// ERROR, MAYBE THE RECORD WASN'T CREATED
			OLogManager.instance().warn(this, "Error on loading index node record %s", e, record.getIdentity());
		}
		record.recycle(this);
		fromStream(record.toStream());
	}

	protected void load(final OStorage iStorage) {
		ORawBuffer raw = iStorage.readRecord(null, (ORecordId) record.getIdentity(), null, null);
		record.fill(null, (ORecordId) record.getIdentity(), raw.version, raw.buffer, false);
		fromStream(raw.buffer);
	}

	public ORID getIdentity() {
		return record.getIdentity();
	}

	@SuppressWarnings("unchecked")
	public K getKeyAt(final int iIndex) {
		K k = keys[iIndex];
		if (k == null)
			try {
				OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.unserializeKey", 1);

				k = (K) keyFromStream(iIndex);

				if (iIndex == 0 || iIndex == size || treeDataProvider.keepKeysInMemory)
					// KEEP THE UNMARSHALLED KEY IN MEMORY. TO OPTIMIZE FIRST AND LAST ITEM ARE ALWAYS KEPT IN MEMORY TO SPEEDUP FREQUENT
					// NODE CHECKING OF BOUNDS
					keys[iIndex] = k;

			} catch (IOException e) {
				OLogManager.instance().error(this, "Can't lazy load the key #" + iIndex + " in tree node " + this, e,
						OSerializationException.class);
			}

		return k;
	}

	@SuppressWarnings("unchecked")
	public V getValueAt(final int iIndex) {
		V v = values[iIndex];
		if (v == null)
			try {
				OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.unserializeValue", 1);

				v = (V) valueFromStream(iIndex);

				if (treeDataProvider.keepValuesInMemory)
					// KEEP THE UNMARSHALLED VALUE IN MEMORY
					values[iIndex] = v;

			} catch (IOException e) {

				OLogManager.instance().error(this, "Can't lazy load the value #" + iIndex + " in tree node " + this, e,
						OSerializationException.class);
			}

		return v;
	}

	public int getSize() {
		return size;
	}

	public int getPageSize() {
		return pageSize;
	}

	public ORID getParent() {
		return parentRid;
	}

	public ORID getLeft() {
		return leftRid;
	}

	public ORID getRight() {
		return rightRid;
	}

	public boolean getColor() {
		return color;
	}

	public boolean setValueAt(int iIndex, V iValue) {
		values[iIndex] = iValue;
		serializedValues[iIndex] = -1;
		return setDirty();
	}

	public boolean insertAt(int iIndex, K iKey, V iValue) {
		if (iIndex < size) {
			// MOVE RIGHT TO MAKE ROOM FOR THE ITEM
			System.arraycopy(keys, iIndex, keys, iIndex + 1, size - iIndex);
			System.arraycopy(values, iIndex, values, iIndex + 1, size - iIndex);
			System.arraycopy(serializedKeys, iIndex, serializedKeys, iIndex + 1, size - iIndex);
			System.arraycopy(serializedValues, iIndex, serializedValues, iIndex + 1, size - iIndex);
		}

		keys[iIndex] = iKey;
		values[iIndex] = iValue;
		serializedKeys[iIndex] = 0;
		serializedValues[iIndex] = 0;
		size++;

		return setDirty();
	}

	public boolean removeAt(int iIndex) {
		if (iIndex == size - 1) {
			// LAST ONE: JUST REMOVE IT
		} else if (iIndex > -1) {
			// SHIFT LEFT THE VALUES
			System.arraycopy(keys, iIndex + 1, keys, iIndex, size - iIndex - 1);
			System.arraycopy(values, iIndex + 1, values, iIndex, size - iIndex - 1);
			System.arraycopy(serializedKeys, iIndex + 1, serializedKeys, iIndex, size - iIndex - 1);
			System.arraycopy(serializedValues, iIndex + 1, serializedValues, iIndex, size - iIndex - 1);
		}

		// FREE RESOURCES
		serializedKeys[size - 1] = 0;
		serializedValues[size - 1] = 0;
		keys[size - 1] = null;
		values[size - 1] = null;
		size--;
		return setDirty();
	}

	public boolean copyDataFrom(OTreeEntryDataProvider<K, V> iFrom, int iStartPosition) {
		OTreeEntryDataProviderGeneric<K, V> parent = (OTreeEntryDataProviderGeneric<K, V>) iFrom;
		size = iFrom.getSize() - iStartPosition;
		System.arraycopy(parent.keys, iStartPosition, keys, 0, size);
		System.arraycopy(parent.values, iStartPosition, values, 0, size);
		System.arraycopy(parent.serializedKeys, iStartPosition, serializedKeys, 0, size);
		System.arraycopy(parent.serializedValues, iStartPosition, serializedValues, 0, size);
		inStream.setSource(parent.inStream.copy());
		return setDirty();
	}

	public boolean truncate(int iNewSize) {
		// TRUNCATE PARENT
		Arrays.fill(keys, iNewSize, size, null);
		Arrays.fill(values, iNewSize, size, null);
		Arrays.fill(serializedKeys, iNewSize, pageSize, 0);
		Arrays.fill(serializedValues, iNewSize, pageSize, 0);
		size = iNewSize;
		return setDirty();
	}

	public boolean copyFrom(OTreeEntryDataProvider<K, V> iSource) {

		final OTreeEntryDataProviderGeneric<K, V> source = (OTreeEntryDataProviderGeneric<K, V>) iSource;

		serializedKeys = new int[source.serializedKeys.length];
		System.arraycopy(source.serializedKeys, 0, serializedKeys, 0, source.serializedKeys.length);

		serializedValues = new int[source.serializedValues.length];
		System.arraycopy(source.serializedValues, 0, serializedValues, 0, source.serializedValues.length);

		keys = (K[]) new Object[source.keys.length];
		System.arraycopy(source.keys, 0, keys, 0, source.keys.length);

		values = (V[]) new Object[source.values.length];
		System.arraycopy(source.values, 0, values, 0, source.values.length);

		size = source.size;
		return setDirty();
	}

	public boolean setLeft(ORID iRid) {
		if (leftRid.equals(iRid))
			return false;
		leftRid.copyFrom(iRid);
		return setDirty();
	}

	public boolean setRight(ORID iRid) {
		if (rightRid.equals(iRid))
			return false;
		rightRid.copyFrom(iRid);
		return setDirty();
	}

	public boolean setParent(ORID iRid) {
		if (parentRid.equals(iRid))
			return false;
		parentRid.copyFrom(iRid);
		return setDirty();
	}

	public boolean setColor(final boolean iColor) {
		this.color = iColor;
		return setDirty();
	}

	public boolean isEntryDirty() {
		return record.isDirty();
	}

	public void save() {
		if (treeDataProvider.storage == null)
			save(treeDataProvider.getDatabase());
		else
			save(treeDataProvider.storage);
	}

	protected void save(ODatabaseRecord iDb) {
		if (iDb == null) {
			throw new IllegalStateException(
					"Current thread has no database setted and the tree can't be saved correctly. Assure to close the database before the application if off.");
		}
		record.setDatabase(iDb);
		record.save(treeDataProvider.clusterName);
	}

	protected void save(OStorage iSt) {
		record.fromStream(toStream());
		if (record.getIdentity().isValid())
			// UPDATE IT WITHOUT VERSION CHECK SINCE ALL IT'S LOCKED
			record.setVersion(iSt.updateRecord((ORecordId) record.getIdentity(), record.toStream(), -1, record.getRecordType(), null));
		else {
			// CREATE IT
			if (record.getIdentity().getClusterId() == ORID.CLUSTER_ID_INVALID)
				((ORecordId) record.getIdentity()).clusterId = treeDataProvider.clusterId;
			record.setIdentity(record.getIdentity().getClusterId(),
					iSt.createRecord((ORecordId) record.getIdentity(), record.toStream(), record.getRecordType(), null));
		}
		record.unsetDirty();
	}

	public void delete() {
		if (treeDataProvider.storage == null)
			delete(treeDataProvider.getDatabase());
		else
			delete(treeDataProvider.storage);
		// FORCE REMOVING OF K/V AND SEIALIZED K/V AS WELL
		keys = null;
		values = null;
		serializedKeys = null;
		serializedValues = null;
	}

	protected void delete(ODatabaseRecord iDb) {
		record.setDatabase(iDb);
		record.delete();
	}

	protected void delete(OStorage iSt) {
		iSt.deleteRecord((ORecordId) record.getIdentity(), record.getVersion(), null);
	}

	public void clear() {
		keys = null;
		values = null;
		if (inStream != null) {
			inStream.close();
			inStream = null;
		}
		serializedKeys = null;
		serializedValues = null;
		record.recycle(null);
		record = null;
		size = 0;
	}

	public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		inStream.setSource(iStream);

		try {
			pageSize = inStream.getAsInteger();

			parentRid = new ORecordId().fromStream(inStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			leftRid = new ORecordId().fromStream(inStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			rightRid = new ORecordId().fromStream(inStream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

			color = inStream.getAsBoolean();
			size = inStream.getAsInteger();

			if (size > pageSize)
				throw new OConfigurationException("Loaded index with page size setted to " + pageSize
						+ " while the loaded was built with: " + size);

			// UNCOMPACT KEYS SEPARATELY
			serializedKeys = new int[pageSize];
			for (int i = 0; i < size; ++i) {
				serializedKeys[i] = inStream.getAsByteArrayOffset();
			}

			// KEYS WILL BE LOADED LAZY
			keys = (K[]) new Object[pageSize];

			// UNCOMPACT VALUES SEPARATELY
			serializedValues = new int[pageSize];
			for (int i = 0; i < size; ++i) {
				serializedValues[i] = inStream.getAsByteArrayOffset();
			}

			// VALUES WILL BE LOADED LAZY
			values = (V[]) new Object[pageSize];

			return this;
		} catch (IOException e) {
			throw new OSerializationException("Can't unmarshall RB+Tree node", e);
		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreeEntryP.fromStream", timer);
		}
	}

	public byte[] toStream() throws OSerializationException {

		// XXX Sylvain : really necessary ?
		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		// final Integer identityRecord = System.identityHashCode(record);
		// final Set<Integer> marshalledRecords = OSerializationThreadLocal.INSTANCE.get();
		// if (marshalledRecords.contains(identityRecord)) {
		// // ALREADY IN STACK, RETURN EMPTY
		// return new byte[] {};
		// } else
		// marshalledRecords.add(identityRecord);

		final long timer = OProfiler.getInstance().startChrono();

		final OMemoryOutputStream outStream = new OMemoryOutputStream();

		try {
			outStream.add(pageSize);

			outStream.addAsFixed(parentRid.toStream());
			outStream.addAsFixed(leftRid.toStream());
			outStream.addAsFixed(rightRid.toStream());

			outStream.add(color);
			outStream.add(size);

			for (int i = 0; i < size; ++i)
				serializedKeys[i] = outStream.add(serializeNewKey(i));

			for (int i = 0; i < size; ++i)
				serializedValues[i] = outStream.add(serializeNewValue(i));

			outStream.flush();

			final byte[] buffer = outStream.getByteArray();

			inStream.setSource(buffer);

			record.fromStream(buffer);
			return buffer;

		} catch (IOException e) {
			throw new OSerializationException("Can't marshall RB+Tree node", e);
		} finally {
			// marshalledRecords.remove(identityRecord);
			OProfiler.getInstance().stopChrono("OMVRBTreeEntryP.toStream", timer);
		}
	}

	/**
	 * Serialize only the new keys or the changed.
	 * 
	 */
	protected byte[] serializeNewKey(final int iIndex) throws IOException {
		if (serializedKeys[iIndex] <= 0) {
			// NEW OR MODIFIED: MARSHALL CONTENT
			OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.serializeValue", 1);
			return treeDataProvider.keySerializer.toStream(null, keys[iIndex]);
		}
		// RETURN ORIGINAL CONTENT
		return inStream.getAsByteArray(serializedKeys[iIndex]);
	}

	/**
	 * Serialize only the new values or the changed.
	 * 
	 */
	protected byte[] serializeNewValue(final int iIndex) throws IOException {
		if (serializedValues[iIndex] <= 0) {
			// NEW OR MODIFIED: MARSHALL CONTENT
			OProfiler.getInstance().updateCounter("OMVRBTreeEntryP.serializeKey", 1);
			return treeDataProvider.valueSerializer.toStream(null, values[iIndex]);
		}
		// RETURN ORIGINAL CONTENT
		return inStream.getAsByteArray(serializedValues[iIndex]);
	}

	protected boolean setDirty() {
		if (record.isDirty())
			return false;
		record.setDirty();
		return true;
	}

	protected Object keyFromStream(final int iIndex) throws IOException {
		return treeDataProvider.keySerializer.fromStream(treeDataProvider.storage == null ? treeDataProvider.getDatabase() : null,
				inStream.getAsByteArray(serializedKeys[iIndex]));
	}

	protected Object valueFromStream(final int iIndex) throws IOException {
		return treeDataProvider.valueSerializer.fromStream(treeDataProvider.storage == null ? treeDataProvider.getDatabase() : null,
				inStream.getAsByteArray(serializedValues[iIndex]));
	}

	public String toString() {
		final StringBuilder buffer = new StringBuilder();
		buffer.append("indexEntry ");
		buffer.append(record.getIdentity());
		buffer.append(" (size=");
		buffer.append(size);
		if (size > 0) {
			buffer.append(" [");
			if (size > 1) {
				buffer.append(getKeyAt(0));
				buffer.append(" ... ");
				buffer.append(getKeyAt(size - 1));
			} else {
				buffer.append(getKeyAt(0));
			}
			buffer.append("]");
		}
		buffer.append(")");
		return buffer.toString();
	}

}
