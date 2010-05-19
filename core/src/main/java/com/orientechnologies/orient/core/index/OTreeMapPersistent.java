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
package com.orientechnologies.orient.core.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.collection.OTreeMap;
import com.orientechnologies.common.collection.OTreeMapEntry;
import com.orientechnologies.common.collection.OTreeMapEventListener;
import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;

@SuppressWarnings("serial")
public class OTreeMapPersistent<K, V> extends OTreeMap<K, V> implements OTreeMapEventListener<K, V>, OSerializableStream {
	private OSharedResourceExternal											lock						= new OSharedResourceExternal();
	protected ODatabaseRecord<?>												database;

	protected OStreamSerializer													keySerializer;
	protected OStreamSerializer													valueSerializer;

	protected final List<OTreeMapEntryPersistent<K, V>>	recordsToCommit	= new ArrayList<OTreeMapEntryPersistent<K, V>>();
	protected final OMemoryOutputStream									entryRecordBuffer;

	protected final String															clusterName;
	private ORecordBytes																record;

	public OTreeMapPersistent(final ODatabaseRecord<?> iDatabase, final String iClusterName, final ORID iRID) {
		this(iDatabase, iClusterName, null, null);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OTreeMapPersistent(final ODatabaseRecord<?> iDatabase, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		// MINIMIZE I/O USING A LARGER PAGE THAN THE DEFAULT USED IN MEMORY
		super(1024, 0.7f);

		database = iDatabase;
		clusterName = iClusterName;
		record = new ORecordBytes(database);

		keySerializer = iKeySerializer;
		valueSerializer = iValueSerializer;

		entryRecordBuffer = new OMemoryOutputStream(getPageSize() * 15);

		setListener(this);
	}

	@Override
	protected OTreeMapEntryPersistent<K, V> createEntry(final K key, final V value) {
		adjustPageSize();
		return new OTreeMapEntryPersistent<K, V>(this, key, value, null);
	}

	@Override
	protected OTreeMapEntry<K, V> createEntry(final OTreeMapEntry<K, V> parent) {
		adjustPageSize();
		return new OTreeMapEntryPersistent<K, V>(parent, parent.getPageSplitItems());
	}

	@Override
	public void clear() {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();

		try {
			if (root != null)
				((OTreeMapEntryPersistent<K, V>) root).delete();

			super.clear();

			getListener().signalTreeChanged(this);

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on deleting the tree: " + record.getIdentity(), e, ODatabaseException.class);
		} finally {

			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("OTreeMapPersistent.clear", timer);
		}
	}

	@Override
	public V put(final K key, final V value) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();

		try {
			ORecord<?> record;

			if (key instanceof ORecord<?>) {
				// RECORD KEY: ASSURE IT'S PERSISTENT TO AVOID STORING INVALID RIDs
				record = (ORecord<?>) key;
				if (!record.getIdentity().isValid())
					record.save();
			}

			if (value instanceof ORecord<?>) {
				// RECORD VALUE: ASSURE IT'S PERSISTENT TO AVOID STORING INVALID RIDs
				record = (ORecord<?>) value;
				if (!record.getIdentity().isValid())
					record.save();
			}

			final V v = super.put(key, value);
			commitChanges(null);
			return v;
		} finally {

			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("OTreeMapPersistent.put", timer);
		}
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> map) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();

		try {
			super.putAll(map);
			commitChanges(null);

		} finally {

			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("OTreeMapPersistent.putAll", timer);
		}
	}

	@Override
	public V remove(final Object key) {
		final long timer = OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();

		try {
			V v = super.remove(key);
			commitChanges(null);
			return v;
		} finally {

			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("remove", timer);
		}
	}

	public void commitChanges(final ODatabaseRecord<?> iDatabase) {
		final long timer = OProfiler.getInstance().startChrono();

		ORecordId rid = null;

		lock.acquireExclusiveLock();

		try {
			// database.begin();

			if (recordsToCommit.size() > 0) {
				// COMMIT BEFORE THE NEW RECORDS (TO ASSURE RID IN RELATIONSHIPS)
				for (OTreeMapEntryPersistent<K, V> node : recordsToCommit) {
					if (node.record.isDirty() && !node.record.getIdentity().isValid()) {
						if (iDatabase != null)
							// REPLACE THE DATABASE WITH THE NEW ACQUIRED
							node.record.setDatabase(iDatabase);

						// CREATE THE RECORD
						node.save();
					}
				}

				// COMMIT THE RECORDS CHANGED
				for (OTreeMapEntryPersistent<K, V> node : recordsToCommit) {
					if (node.record.isDirty() && node.record.getIdentity().isValid()) {
						if (iDatabase != null)
							// REPLACE THE DATABASE WITH THE NEW ACQUIRED
							node.record.setDatabase(iDatabase);

						// UPDATE THE RECORD
						node.save();
					}
				}

				recordsToCommit.clear();
			}

			if (record.isDirty()) {
				// TREE IS CHANGED AS WELL
				if (iDatabase != null)
					// REPLACE THE DATABASE WITH THE NEW ACQUIRED
					record.setDatabase(iDatabase);

				save();
			}

			// database.commit();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on saving the tree", e, ODatabaseException.class);

			if (iDatabase != null)
				iDatabase.rollback();
			else
				database.rollback();

		} finally {

			lock.releaseExclusiveLock();

			OProfiler.getInstance().stopChrono("OTreeMapPersistent.commitChanges", timer);
		}
	}

	public OSerializableStream fromStream(byte[] iStream) throws IOException {
		final long timer = OProfiler.getInstance().startChrono();
		try {
			OMemoryInputStream stream = new OMemoryInputStream(iStream);

			ORID rootRid = new ORecordId().fromStream(stream.getAsByteArray());

			size = stream.getAsInteger();
			lastPageSize = stream.getAsShort();

			keySerializer = OStreamSerializerFactory.get(database, stream.getAsString());
			valueSerializer = OStreamSerializerFactory.get(database, stream.getAsString());

			// LOAD THE ROOT OBJECT AFTER ALL
			if (rootRid.isValid())
				root = new OTreeMapEntryPersistent<K, V>(this, null, rootRid);

			return this;

		} catch (Exception e) {

			OLogManager.instance().error(this, "Error on unmarshalling OTreeMapPersistent object", e, OSerializationException.class);

		} finally {
			OProfiler.getInstance().stopChrono("OTreeMapPersistent.fromStream", timer);
		}
		return this;
	}

	public byte[] toStream() throws IOException {
		final long timer = OProfiler.getInstance().startChrono();
		try {
			OMemoryOutputStream stream = new OMemoryOutputStream();

			if (root != null) {
				OTreeMapEntryPersistent<K, V> pRoot = (OTreeMapEntryPersistent<K, V>) root;
				if (!pRoot.record.getIdentity().isValid()) {
					// FIRST TIME: SAVE IT
					pRoot.save();
				}

				stream.add(pRoot.record.getIdentity().toStream());
			} else
				stream.add(ORecordId.EMPTY_RECORD_ID_STREAM);

			stream.add(size);
			stream.add((short) lastPageSize);

			stream.add(keySerializer.getName());
			stream.add(valueSerializer.getName());

			return stream.getByteArray();

		} finally {
			OProfiler.getInstance().stopChrono("OTreeMapPersistent.toStream", timer);
		}
	}

	public void signalTreeChanged(OTreeMap<K, V> iTree) {
		record.setDirty();
	}

	public void signalNodeChanged(OTreeMapEntry<K, V> iNode) {
		recordsToCommit.add((OTreeMapEntryPersistent<K, V>) iNode);
	}

	@Override
	public int hashCode() {
		final ORID rid = record.getIdentity();
		return rid == null ? 0 : rid.hashCode();
	}

	public OTreeMapPersistent<K, V> load() throws IOException {
		lock.acquireExclusiveLock();

		try {
			record.load();
			fromStream(record.toStream());
			return this;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public OTreeMapPersistent<K, V> save() throws IOException {

		lock.acquireExclusiveLock();

		try {
			record.fromStream(toStream());
			record.save(clusterName);
			return this;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	public ORecordBytes getRecord() {
		return record;
	}

	private void adjustPageSize() {
		lastPageSize = (int) (size * 0.2 / 100);
		if (lastPageSize < 256)
			lastPageSize = 256;
	}

	@Override
	public V get(Object key) {
		lock.acquireSharedLock();

		try {
			return super.get(key);

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public boolean containsKey(Object key) {
		lock.acquireSharedLock();

		try {
			return super.containsKey(key);

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public boolean containsValue(Object value) {
		lock.acquireSharedLock();

		try {
			return super.containsValue(value);

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		lock.acquireSharedLock();

		try {
			return super.entrySet();

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public Set<K> keySet() {
		lock.acquireSharedLock();

		try {
			return super.keySet();

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public Collection<V> values() {
		lock.acquireSharedLock();

		try {
			return super.values();

		} finally {
			lock.releaseSharedLock();
		}
	}
}
