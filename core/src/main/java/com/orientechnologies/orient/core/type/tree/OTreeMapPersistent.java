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
package com.orientechnologies.orient.core.type.tree;

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
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLogical;

/**
 * Persistent TreeMap implementation. The difference with the class OTreeMapPersistent is the level. In facts this class works
 * directly at the storage level, while the other at database level. This class is used for Logical Clusters. It can'be
 * transactional. It uses the entryPoints linked list to get the best entry point for searching a node.
 * 
 * @see OClusterLogical
 */
@SuppressWarnings("serial")
public abstract class OTreeMapPersistent<K, V> extends OTreeMap<K, V> implements OTreeMapEventListener<K, V>, OSerializableStream {
	protected int																						optimizeThreshold					= 50000;

	protected OSharedResourceExternal												lock											= new OSharedResourceExternal();

	protected OStreamSerializer															keySerializer;
	protected OStreamSerializer															valueSerializer;

	protected final List<OTreeMapEntryPersistent<K, V>>			recordsToCommit						= new ArrayList<OTreeMapEntryPersistent<K, V>>();
	protected final OMemoryOutputStream											entryRecordBuffer;

	protected final String																	clusterName;
	protected ORecordBytes																	record;
	protected String																				fetchPlan;
	protected volatile int																	usageCounter							= 0;

	// STORES IN MEMORY DIRECT REFERENCES TO PORTION OF THE TREE
	protected int																						entryPointSize						= 20;
	protected float																					entryPointThresholdFactor	= 1.5f;
	protected volatile List<OTreeMapEntryPersistent<K, V>>	entryPoints								= new ArrayList<OTreeMapEntryPersistent<K, V>>(
																																												entryPointSize);
	protected List<OTreeMapEntryPersistent<K, V>>						tmpEntryPoints						= new ArrayList<OTreeMapEntryPersistent<K, V>>(
																																												entryPointSize);

	public OTreeMapPersistent(final String iClusterName, final ORID iRID) {
		this(iClusterName, null, null);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OTreeMapPersistent(String iClusterName, final OStreamSerializer iKeySerializer, final OStreamSerializer iValueSerializer) {
		// MINIMIZE I/O USING A LARGER PAGE THAN THE DEFAULT USED IN MEMORY
		super(1024, 0.7f);

		clusterName = iClusterName;
		record = new ORecordBytes();

		keySerializer = iKeySerializer;
		valueSerializer = iValueSerializer;

		entryRecordBuffer = new OMemoryOutputStream(getPageSize() * 15);

		setListener(this);
	}

	public abstract OTreeMapPersistent<K, V> load() throws IOException;

	public abstract OTreeMapPersistent<K, V> save() throws IOException;

	protected abstract void serializerFromStream(OMemoryInputStream stream) throws IOException;

	/**
	 * Lazy loads a node.
	 */
	protected abstract OTreeMapEntryPersistent<K, V> createEntry(OTreeMapEntryPersistent<K, V> iParent, ORID iRecordId)
			throws IOException;

	@Override
	public void clear() {
		final long timer = OProfiler.getInstance().startChrono();
		lock.acquireExclusiveLock();

		try {
			if (root != null)
				((OTreeMapEntryPersistent<K, V>) root).delete();

			super.clear();

			getListener().signalTreeChanged(this);

			usageCounter = 0;
			entryPoints.clear();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on deleting the tree: " + record.getIdentity(), e, OStorageException.class);
		} finally {

			lock.releaseExclusiveLock();
			OProfiler.getInstance().stopChrono("OTreeMapPersistent.clear", timer);
		}
	}

	/**
	 * Optimize the tree memory consumption by keeping part of nodes as entry points and clearing all the rest.
	 */
	public void optimize() {
		final long timer = System.currentTimeMillis();// OProfiler.getInstance().startChrono();

		lock.acquireExclusiveLock();

		try {
			if (root == null)
				return;

			OLogManager.instance().debug(this, "Starting optimization of RB+Tree...");

			if (entryPoints.size() == 0)
				// FIRST TIME THE LIST IS NULL: START FROM ROOT
				entryPoints.add((OTreeMapEntryPersistent<K, V>) root);

			// COUNT ALL IN-MEMORY NODES BY BROWSING ALL THE ENTRYPOINT NODES
			int nodes = 0;
			// final List<OTreeMapEntryPersistent<K, V>> tmp = new ArrayList<OTreeMapEntryPersistent<K, V>>();

			for (OTreeMapEntryPersistent<K, V> entryPoint : entryPoints)
				for (OTreeMapEntryPersistent<K, V> e = entryPoint; e != null; e = e.getNextInMemory()) {
					// for (OTreeMapEntryPersistent<K, V> t : tmp)
					// if (t == e)
					// OLogManager.instance().error(this, "Node ERROR");
					// tmp.add(e);
					++nodes;
				}

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Found %d nodes in memory, %d items on disk, threshold=%d, entryPoints=%d", nodes, size,
						(entryPointSize * entryPointThresholdFactor), entryPoints.size());

			if (nodes < entryPointSize * entryPointThresholdFactor)
				// UNDER THRESHOLD AVOID TO OPTIMIZE
				return;

			// COMPUTE THE DISTANCE BETWEEN NODES
			final int distance;
			if (nodes <= entryPointSize)
				distance = 1;
			else
				distance = nodes / entryPointSize + 1;

			tmpEntryPoints.clear();

			OLogManager.instance().debug(this, "Compacting nodes with distance = %d", distance);

			// RESET ALL THE IN-MEMORY NODES NOT IN THE ENTRY POINT LIST
			int nodeCounter = 0;
			OTreeMapEntryPersistent<K, V> lastNode = null;
			for (OTreeMapEntryPersistent<K, V> entryPoint : entryPoints)
				for (OTreeMapEntryPersistent<K, V> e = entryPoint; e != null; e = e.getNextInMemory()) {
					++nodeCounter;

					if (tmpEntryPoints.size() == 0 || nodeCounter % distance == 0)
						tmpEntryPoints.add(e);

					lastNode = e;
				}

			if (tmpEntryPoints.size() > 1 && tmpEntryPoints.get(tmpEntryPoints.size() - 1) != lastNode)
				// ADD THE LAST ONE IF IT'S NOT YET PRESENT
				tmpEntryPoints.add(lastNode);

			// SWAP TMP AND REAL ENTRY POINT COLLECTIONS
			final List<OTreeMapEntryPersistent<K, V>> a = entryPoints;
			entryPoints = tmpEntryPoints;
			tmpEntryPoints = a;
			tmpEntryPoints.clear();

			// FREE ALL THE NODES BUT THE ENTRY POINTS
			for (OTreeMapEntryPersistent<K, V> entryPoint : entryPoints)
				entryPoint.disconnect();

			if (OLogManager.instance().isDebugEnabled()) {
				// COUNT ALL IN-MEMORY NODES BY BROWSING ALL THE ENTRYPOINT NODES
				nodes = 0;
				for (OTreeMapEntryPersistent<K, V> entryPoint : entryPoints)
					for (OTreeMapEntryPersistent<K, V> e = entryPoint; e != null; e = e.getNextInMemory())
						++nodes;

				OLogManager.instance().debug(this, "Now Found %d nodes in memory and threshold=%d. EntryPoints=%d", nodes,
						(entryPointSize * entryPointThresholdFactor), entryPoints.size());
			}
		} finally {

			lock.releaseExclusiveLock();
			OProfiler.getInstance().stopChrono("OTreeMapPersistent.optimize", timer);

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Optimization completed in %d ms\n", System.currentTimeMillis() - timer);
		}
	}

	@Override
	public V put(final K key, final V value) {
		final long timer = OProfiler.getInstance().startChrono();

		updateUsageCounter();

		lock.acquireExclusiveLock();

		try {
			final V v = internalPut(key, value);
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

		updateUsageCounter();

		lock.acquireExclusiveLock();

		try {
			for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
				internalPut(entry.getKey(), entry.getValue());
			}
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
		lock.acquireExclusiveLock();

		try {
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

		} catch (IOException e) {
			OLogManager.instance().exception("Error on saving the tree", e, OStorageException.class);

		} finally {

			lock.releaseExclusiveLock();
			OProfiler.getInstance().stopChrono("OTreeMapPersistent.commitChanges", timer);
		}
	}

	public OSerializableStream fromStream(final byte[] iStream) throws IOException {
		final long timer = OProfiler.getInstance().startChrono();

		final ORID rootRid = new ORecordId();

		try {
			final OMemoryInputStream stream = new OMemoryInputStream(iStream);

			rootRid.fromStream(stream.getAsByteArray());

			size = stream.getAsInteger();
			lastPageSize = stream.getAsShort();

			serializerFromStream(stream);

			// LOAD THE ROOT OBJECT AFTER ALL
			if (rootRid.isValid())
				root = createEntry(null, rootRid);

			return this;

		} catch (Exception e) {

			OLogManager.instance().error(this, "Error on unmarshalling OTreeMapPersistent object from record: " + rootRid, e,
					OSerializationException.class);

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

	public void signalTreeChanged(final OTreeMap<K, V> iTree) {
		record.setDirty();
	}

	public void signalNodeChanged(final OTreeMapEntry<K, V> iNode) {
		recordsToCommit.add((OTreeMapEntryPersistent<K, V>) iNode);
	}

	@Override
	public int hashCode() {
		final ORID rid = record.getIdentity();
		return rid == null ? 0 : rid.hashCode();
	}

	public ORecordBytes getRecord() {
		return record;
	}

	protected void adjustPageSize() {
		// int newPageSize = (int) (size * 0.2 / 100);
		// if (newPageSize > lastPageSize)
		// lastPageSize = newPageSize;
	}

	@Override
	public V get(final Object iKey) {
		updateUsageCounter();

		lock.acquireSharedLock();

		try {
			return super.get(iKey);

		} finally {
			lock.releaseSharedLock();
		}
	}

	public V get(final Object iKey, final String iFetchPlan) {
		fetchPlan = iFetchPlan;
		return get(iKey);
	}

	@Override
	public boolean containsKey(final Object key) {
		updateUsageCounter();

		lock.acquireSharedLock();

		try {
			return super.containsKey(key);

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public boolean containsValue(final Object value) {
		updateUsageCounter();

		lock.acquireSharedLock();

		try {
			return super.containsValue(value);

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		updateUsageCounter();

		lock.acquireSharedLock();

		try {
			return super.entrySet();

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public Set<K> keySet() {
		updateUsageCounter();

		lock.acquireSharedLock();

		try {
			return super.keySet();

		} finally {
			lock.releaseSharedLock();
		}
	}

	@Override
	public Collection<V> values() {
		updateUsageCounter();

		lock.acquireSharedLock();

		try {
			return super.values();

		} finally {
			lock.releaseSharedLock();
		}
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public void setFetchPlan(String fetchPlan) {
		this.fetchPlan = fetchPlan;
	}

	public int getOptimizeThreshold() {
		return optimizeThreshold;
	}

	public void setOptimizeThreshold(int optimizeThreshold) {
		this.optimizeThreshold = optimizeThreshold;
	}

	public int getEntryPointSize() {
		return entryPointSize;
	}

	public void setEntryPointSize(int entryPointSize) {
		this.entryPointSize = entryPointSize;
	}

	private V internalPut(final K key, final V value) {
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

		return super.put(key, value);
	}

	/**
	 * Updates the usage counter and check if it's higher than the configured threshold. In this case executes the optimization and
	 * reset the usage counter.
	 */
	protected void updateUsageCounter() {
		usageCounter++;
		if (optimizeThreshold > 0 && usageCounter > optimizeThreshold) {
			optimize();
			usageCounter = 0;
		}
	}

	/**
	 * Returns the best entry point to start the search.
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected OTreeMapEntry<K, V> getBestEntryPoint(final Object iKey) {
		final Comparable<? super K> key = (Comparable<? super K>) iKey;

		OTreeMapEntryPersistent<K, V> bestNode = null;

		if (entryPoints.size() == 0)
			// TREE EMPTY: RETURN ROOT
			return root;

		// SEARCH THE BEST KEY
		OTreeMapEntryPersistent<K, V> e;
		for (int i = 0; i < entryPoints.size(); ++i) {
			e = entryPoints.get(i);

			if (e.serializedKeys == null) {
				// CLEAN WRONG ENTRY (WHY THEY ARE WRONG?)
				entryPoints.remove(i);
				--i;
				continue;
			}

			if (key.compareTo(e.getFirstKey()) < 0) {
				// RETURN THE PREVIOUS ONE OF CURRENT IF IT'S NULL
				return bestNode != null ? bestNode : e;
			} else if (key.compareTo(e.getFirstKey()) >= 0 && key.compareTo(e.getLastKey()) <= 0)
				// PERFECT MATCH, VERY LUCKY: RETURN THE CURRENT = 0 READS
				return e;

			// SET THE CURRENT AS BEST NODE
			bestNode = e;
		}

		// RETURN THE LATEST ONE
		return bestNode;
	}

	/**
	 * Remove an entry point from the list
	 */
	void removeEntryPoint(final OTreeMapEntryPersistent<K, V> iEntry) {
		for (int i = 0; i < entryPoints.size(); ++i)
			if (entryPoints.get(i) == iEntry) {
				entryPoints.remove(i);
				break;
			}
	}

	/**
	 * Returns the most left key in memory given a root node.
	 */
	private final OTreeMapEntryPersistent<K, V> getMostLeftNode(final OTreeMapEntryPersistent<K, V> iEntryPoint) {
		if (iEntryPoint == null)
			return null;

		OTreeMapEntryPersistent<K, V> p = iEntryPoint;

		// GET THE HIGHEST NODE
		while (p.parent != null)
			p = p.parent;

		// GO DOWN TO GET THE MOST LEFT
		while (p.left != null)
			p = p.left;

		return p;
	}
}
