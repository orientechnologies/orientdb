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
import java.util.Arrays;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

public class OMVRBTreeMapEntryProvider<K, V> extends OMVRBTreeEntryDataProviderBinary<K, V> {
	private static final long	serialVersionUID	= 1L;
	protected K[]							keys;
	protected V[]							values;
	protected int[]						serializedKeys;
	protected int[]						serializedValues;

	@SuppressWarnings("unchecked")
	public OMVRBTreeMapEntryProvider(final OMVRBTreeMapProvider<K, V> iTreeDataProvider) {
		super(iTreeDataProvider);
		keys = (K[]) new Object[pageSize];
		values = (V[]) new Object[pageSize];
		serializedKeys = new int[pageSize];
		serializedValues = new int[pageSize];
		parentRid = new ORecordId();
		leftRid = new ORecordId();
		rightRid = new ORecordId();
	}

	public OMVRBTreeMapEntryProvider(final OMVRBTreeMapProvider<K, V> iTreeDataProvider, final ORID iRID) {
		super(iTreeDataProvider, iRID);
	}

	@SuppressWarnings("unchecked")
	public K getKeyAt(final int iIndex) {
		K k = keys[iIndex];
		if (k == null)
			try {
				OProfiler.getInstance().updateCounter("OMVRBTreeMapEntry.unserializeKey", 1);

				k = (K) keyFromStream(iIndex);

				if (iIndex == 0 || iIndex == size || ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keepKeysInMemory)
					// KEEP THE UNMARSHALLED KEY IN MEMORY. TO OPTIMIZE FIRST AND LAST ITEM ARE ALWAYS KEPT IN MEMORY TO SPEEDUP FREQUENT
					// NODE CHECKING OF BOUNDS
					keys[iIndex] = k;

			} catch (IOException e) {
				OLogManager.instance().error(this, "Cannot lazy load the key #" + iIndex + " in tree node " + this, e,
						OSerializationException.class);
			}

		return k;
	}

	@SuppressWarnings("unchecked")
	public V getValueAt(final int iIndex) {
		V v = values[iIndex];
		if (v == null)
			try {
				OProfiler.getInstance().updateCounter("OMVRBTreeMapEntry.unserializeValue", 1);

				v = (V) valueFromStream(iIndex);

				if (((OMVRBTreeMapProvider<K, V>) treeDataProvider).keepValuesInMemory)
					// KEEP THE UNMARSHALLED VALUE IN MEMORY
					values[iIndex] = v;

			} catch (IOException e) {

				OLogManager.instance().error(this, "Cannot lazy load the value #" + iIndex + " in tree node " + this, e,
						OSerializationException.class);
			}

		return v;
	}

	public boolean setValueAt(int iIndex, final V iValue) {
		values[iIndex] = iValue;
		serializedValues[iIndex] = -1;
		return setDirty();
	}

	public boolean insertAt(final int iIndex, final K iKey, final V iValue) {
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

	public boolean removeAt(final int iIndex) {
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

	public boolean copyDataFrom(final OMVRBTreeEntryDataProvider<K, V> iFrom, int iStartPosition) {
		OMVRBTreeMapEntryProvider<K, V> parent = (OMVRBTreeMapEntryProvider<K, V>) iFrom;
		size = iFrom.getSize() - iStartPosition;
		System.arraycopy(parent.keys, iStartPosition, keys, 0, size);
		System.arraycopy(parent.values, iStartPosition, values, 0, size);
		System.arraycopy(parent.serializedKeys, iStartPosition, serializedKeys, 0, size);
		System.arraycopy(parent.serializedValues, iStartPosition, serializedValues, 0, size);
		stream.setSource(parent.stream.copy());
		return setDirty();
	}

	public boolean truncate(final int iNewSize) {
		// TRUNCATE PARENT
		Arrays.fill(keys, iNewSize, size, null);
		Arrays.fill(values, iNewSize, size, null);
		Arrays.fill(serializedKeys, iNewSize, pageSize, 0);
		Arrays.fill(serializedValues, iNewSize, pageSize, 0);
		size = iNewSize;
		return setDirty();
	}

	@SuppressWarnings("unchecked")
	public boolean copyFrom(final OMVRBTreeEntryDataProvider<K, V> iSource) {
		final OMVRBTreeMapEntryProvider<K, V> source = (OMVRBTreeMapEntryProvider<K, V>) iSource;

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

	@Override
	public void delete() {
		super.delete();
		// FORCE REMOVING OF K/V AND SEIALIZED K/V AS WELL
		keys = null;
		values = null;
		serializedKeys = null;
		serializedValues = null;
	}

	@Override
	public void clear() {
		super.clear();
		keys = null;
		values = null;
		serializedKeys = null;
		serializedValues = null;
	}

	@SuppressWarnings("unchecked")
	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		stream.setSource(iStream);

		try {
			pageSize = stream.getAsInteger();

			parentRid = new ORecordId().fromStream(stream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			leftRid = new ORecordId().fromStream(stream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));
			rightRid = new ORecordId().fromStream(stream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

			color = stream.getAsBoolean();
			size = stream.getAsInteger();

			if (size > pageSize)
				throw new OConfigurationException("Loaded index with page size setted to " + pageSize
						+ " while the loaded was built with: " + size);

			// UNCOMPACT KEYS SEPARATELY
			serializedKeys = new int[pageSize];
			for (int i = 0; i < size; ++i) {
				serializedKeys[i] = stream.getAsByteArrayOffset();
			}

			// KEYS WILL BE LOADED LAZY
			keys = (K[]) new Object[pageSize];

			// UNCOMPACT VALUES SEPARATELY
			serializedValues = new int[pageSize];
			for (int i = 0; i < size; ++i) {
				serializedValues[i] = stream.getAsByteArrayOffset();
			}

			// VALUES WILL BE LOADED LAZY
			values = (V[]) new Object[pageSize];

			return this;
		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreeMapEntry.fromStream", timer);
		}
	}

	public byte[] toStream() throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			final OMemoryStream outStream = new OMemoryStream();
			outStream.jump(0);
			outStream.set(pageSize);

			outStream.setAsFixed(parentRid.toStream());
			outStream.setAsFixed(leftRid.toStream());
			outStream.setAsFixed(rightRid.toStream());

			outStream.set(color);
			outStream.set(size);

			for (int i = 0; i < size; ++i)
				serializedKeys[i] = outStream.set(serializeNewKey(i));

			for (int i = 0; i < size; ++i)
				serializedValues[i] = outStream.set(serializeNewValue(i));

			final byte[] buffer = outStream.toByteArray();

			stream.setSource(buffer);
			record.fromStream(buffer);
			return buffer;

		} catch (IOException e) {
			throw new OSerializationException("Cannot marshall RB+Tree node", e);
		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreeMapEntry.toStream", timer);
		}
	}

	/**
	 * Serialize only the new keys or the changed.
	 * 
	 */
	protected byte[] serializeNewKey(final int iIndex) throws IOException {
		if (serializedKeys[iIndex] <= 0) {
			// NEW OR MODIFIED: MARSHALL CONTENT
			OProfiler.getInstance().updateCounter("OMVRBTreeMapEntry.serializeValue", 1);
			return ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer.toStream(null, keys[iIndex]);
		}
		// RETURN ORIGINAL CONTENT
		return stream.getAsByteArray(serializedKeys[iIndex]);
	}

	/**
	 * Serialize only the new values or the changed.
	 * 
	 */
	protected byte[] serializeNewValue(final int iIndex) throws IOException {
		if (serializedValues[iIndex] <= 0) {
			// NEW OR MODIFIED: MARSHALL CONTENT
			OProfiler.getInstance().updateCounter("OMVRBTreeMapEntry.serializeKey", 1);
			return ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer.toStream(null, values[iIndex]);
		}
		// RETURN ORIGINAL CONTENT
		return stream.getAsByteArray(serializedValues[iIndex]);
	}

	protected Object keyFromStream(final int iIndex) throws IOException {
		return ((OMVRBTreeMapProvider<K, V>) treeDataProvider).keySerializer.fromStream(
				treeDataProvider.storage == null ? treeDataProvider.getDatabase() : null, stream.getAsByteArray(serializedKeys[iIndex]));
	}

	protected Object valueFromStream(final int iIndex) throws IOException {
		return ((OMVRBTreeMapProvider<K, V>) treeDataProvider).valueSerializer.fromStream(
				treeDataProvider.storage == null ? treeDataProvider.getDatabase() : null, stream.getAsByteArray(serializedValues[iIndex]));
	}
}
