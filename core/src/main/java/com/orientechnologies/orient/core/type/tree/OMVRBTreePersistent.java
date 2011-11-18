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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.common.collection.OMVRBTreeEventListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.memory.OLowMemoryException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;

/**
 * Persistent based MVRB-Tree implementation. The difference with the class OMVRBTreePersistent is the level. In facts this class
 * works directly at the storage level, while the other at database level. This class is used for Logical Clusters. It can'be
 * transactional. It uses the entryPoints linked list to get the best entry point for searching a node.
 * 
 */
@SuppressWarnings("serial")
public abstract class OMVRBTreePersistent<K, V> extends OMVRBTree<K, V> implements OMVRBTreeEventListener<K, V>,
		OSerializableStream {
	public final static byte																	CURRENT_PROTOCOL_VERSION	= 0;

	protected OStreamSerializer																keySerializer;
	protected OStreamSerializer																valueSerializer;
	protected final Set<OMVRBTreeEntryPersistent<K, V>>				recordsToCommit						= new HashSet<OMVRBTreeEntryPersistent<K, V>>();
	protected final String																		clusterName;
	protected ORecordBytesLazy																record;
	protected String																					fetchPlan;

	// STORES IN MEMORY DIRECT REFERENCES TO PORTION OF THE TREE
	protected int																							optimizeThreshold;
	protected volatile int																		optimization							= 0;
	private int																								insertionCounter					= 0;
	protected int																							entryPointsSize;

	protected float																						optimizeEntryPointsFactor;
	private final TreeMap<K, OMVRBTreeEntryPersistent<K, V>>	entryPoints								= new TreeMap<K, OMVRBTreeEntryPersistent<K, V>>();
	private final Map<ORID, OMVRBTreeEntryPersistent<K, V>>		cache											= new HashMap<ORID, OMVRBTreeEntryPersistent<K, V>>();
	private final OMemoryOutputStream													entryRecordBuffer;
	private static final int																	OPTIMIZE_MAX_RETRY				= 10;

	public OMVRBTreePersistent(final String iClusterName, final ORID iRID) {
		this(iClusterName, null, null);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OMVRBTreePersistent(String iClusterName, final OStreamSerializer iKeySerializer, final OStreamSerializer iValueSerializer) {
		// MINIMIZE I/O USING A LARGER PAGE THAN THE DEFAULT USED IN MEMORY
		super(OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.getValueAsInteger(), (Float) OGlobalConfiguration.MVRBTREE_LOAD_FACTOR
				.getValue());
		config();

		clusterName = iClusterName;
		record = new ORecordBytesLazy(this);

		keySerializer = iKeySerializer;
		valueSerializer = iValueSerializer;

		entryRecordBuffer = new OMemoryOutputStream(getPageSize() * 15);

		setListener(this);
	}

	public abstract OMVRBTreePersistent<K, V> load() throws IOException;

	public abstract OMVRBTreePersistent<K, V> save() throws IOException;

	/**
	 * Lazy loads a node.
	 */
	protected OMVRBTreeEntryPersistent<K, V> loadEntry(final OMVRBTreeEntryPersistent<K, V> iParent, final ORID iRecordId)
			throws IOException {
		// SEARCH INTO THE CACHE
		OMVRBTreeEntryPersistent<K, V> entry = searchNodeInCache(iRecordId);
		if (entry == null) {
			// NOT FOUND: CREATE IT AND PUT IT INTO THE CACHE
			entry = createEntry(iParent, iRecordId);
			addNodeInCache(entry);

			// RECONNECT THE LOADED NODE WITH IN-MEMORY PARENT, LEFT AND RIGHT
			if (entry.parent == null && entry.parentRid.isValid()) {
				// TRY TO ASSIGN THE PARENT IN CACHE IF ANY
				final OMVRBTreeEntryPersistent<K, V> parentNode = searchNodeInCache(entry.parentRid);
				if (parentNode != null)
					entry.setParent(parentNode);
			}

			if (entry.left == null && entry.leftRid.isValid()) {
				// TRY TO ASSIGN THE PARENT IN CACHE IF ANY
				final OMVRBTreeEntryPersistent<K, V> leftNode = searchNodeInCache(entry.leftRid);
				if (leftNode != null)
					entry.setLeft(leftNode);
			}

			if (entry.right == null && entry.rightRid.isValid()) {
				// TRY TO ASSIGN THE PARENT IN CACHE IF ANY
				final OMVRBTreeEntryPersistent<K, V> rightNode = searchNodeInCache(entry.rightRid);
				if (rightNode != null)
					entry.setRight(rightNode);
			}

		} else {
			// COULD BE A PROBLEM BECAUSE IF A NODE IS DISCONNECTED CAN IT STAY IN CACHE?
			// entry.load();
			if (iParent != null)
				// FOUND: ASSIGN IT ONLY IF NOT NULL
				entry.setParent(iParent);
		}

		entry.checkEntryStructure();

		return entry;
	}

	/**
	 * Create a new entry for {@link #loadEntry(OMVRBTreeEntryPersistent, ORID)}.
	 */
	protected abstract OMVRBTreeEntryPersistent<K, V> createEntry(OMVRBTreeEntryPersistent<K, V> iParent, ORID iRecordId)
			throws IOException;

	@Override
	public void clear() {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			if (root != null) {
				((OMVRBTreeEntryPersistent<K, V>) root).delete();
				super.clear();
				getListener().signalTreeChanged(this);
			}

			recordsToCommit.clear();
			entryPoints.clear();
			cache.clear();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on deleting the tree: " + record.getIdentity(), e, OStorageException.class);
		} finally {

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.clear", timer);
		}
	}

	/**
	 * Unload all the in-memory nodes. This is called on transaction rollback.
	 */
	public void unload() {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			// DISCONNECT ALL THE NODES
			for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints.values())
				entryPoint.disconnectLinked(true);
			entryPoints.clear();
			cache.clear();

			recordsToCommit.clear();
			root = null;

			load();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on unload the tree: " + record.getIdentity(), e, OStorageException.class);
		} finally {

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.unload", timer);
		}
	}

	/**
	 * Calls the optimization in soft mode: free resources only if needed.
	 */
	protected void optimize() {
		optimize(false);
	}

	/**
	 * Optimizes the memory needed by the tree in memory by reducing the number of entries to the configured size.
	 * 
	 * @return The total freed nodes
	 */
	public int optimize(final boolean iForce) {
		if (optimization == -1)
			// IS ALREADY RUNNING
			return 0;

		if (!iForce && optimization == 0)
			// NO OPTIMIZATION IS NEEDED
			return 0;

		// SET OPTIMIZATION STATUS AS RUNNING
		optimization = -1;

		final long timer = OProfiler.getInstance().startChrono();

		try {
			if (root == null)
				return 0;

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Starting optimization of MVRB+Tree with %d items in memory...", cache.size());

			// printInMemoryStructure();

			if (entryPoints.size() == 0)
				// FIRST TIME THE LIST IS NULL: START FROM ROOT
				addNodeAsEntrypoint((OMVRBTreeEntryPersistent<K, V>) root);

			// RECONFIG IT TO CATCH CHANGED VALUES
			config();

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Found %d items on disk, threshold=%f, entryPoints=%d, nodesInCache=%d", size,
						(entryPointsSize * optimizeEntryPointsFactor), entryPoints.size(), cache.size());

			final int nodesInMemory = cache.size();

			if (!iForce && nodesInMemory < entryPointsSize * optimizeEntryPointsFactor)
				// UNDER THRESHOLD AVOID TO OPTIMIZE
				return 0;

			lastSearchFound = false;
			lastSearchKey = null;
			lastSearchNode = null;

			int totalDisconnected = 0;

			if (nodesInMemory > entryPointsSize) {
				// REDUCE THE ENTRYPOINTS
				final int distance = nodesInMemory / entryPointsSize + 1;

				final Set<OMVRBTreeEntryPersistent<K, V>> entryPointsToRemove = new HashSet<OMVRBTreeEntryPersistent<K, V>>(nodesInMemory
						- entryPointsSize + 2);

				// REMOVE ENTRYPOINTS AT THE SAME DISTANCE
				int currNode = 0;
				for (final Iterator<OMVRBTreeEntryPersistent<K, V>> it = entryPoints.values().iterator(); it.hasNext();) {
					final OMVRBTreeEntryPersistent<K, V> currentNode = it.next();

					// JUMP THE FIRST (1 can't never be the % of distance) THE LAST, ROOT AND LAST USED
					if (currentNode != root && currentNode != lastSearchNode && it.hasNext())
						if (++currNode % distance != 0) {
							// REMOVE THE NODE
							entryPointsToRemove.add(currentNode);
							it.remove();
						}
				}
				addNodeAsEntrypoint((OMVRBTreeEntryPersistent<K, V>) lastSearchNode);
				addNodeAsEntrypoint((OMVRBTreeEntryPersistent<K, V>) root);

				// DISCONNECT THE REMOVED NODES
				for (OMVRBTreeEntryPersistent<K, V> currentNode : entryPointsToRemove)
					totalDisconnected += currentNode.disconnectLinked(false);
			}

			if (isRuntimeCheckEnabled()) {
				for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints.values())
					for (OMVRBTreeEntryPersistent<K, V> e = (OMVRBTreeEntryPersistent<K, V>) entryPoint.getFirstInMemory(); e != null; e = e
							.getNextInMemory())
						e.checkEntryStructure();
			}

			// COUNT ALL IN-MEMORY NODES BY BROWSING ALL THE ENTRYPOINT NODES
			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "After optimization: %d items on disk, threshold=%f, entryPoints=%d, nodesInCache=%d",
						size, (entryPointsSize * optimizeEntryPointsFactor), entryPoints.size(), cache.size());

			if (debug) {
				int i = 0;
				for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints.values())
					System.out.println("- Entrypoint " + ++i + "/" + entryPoints.size() + ": " + entryPoint);
			}

			return totalDisconnected;
		} finally {
			optimization = 0;
			if (isRuntimeCheckEnabled()) {
				if (!entryPoints.isEmpty())
					for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints.values())
						checkTreeStructure(entryPoint.getFirstInMemory());
				else
					checkTreeStructure(root);
			}

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.optimize", timer);

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Optimization completed in %d ms\n", System.currentTimeMillis() - timer);
		}
	}

	@Override
	public V put(final K key, final V value) {
		optimize();
		final long timer = OProfiler.getInstance().startChrono();

		try {
			final V v = internalPut(key, value);
			commitChanges();
			return v;
		} finally {

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.put", timer);
		}
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> map) {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
				internalPut(entry.getKey(), entry.getValue());
			}
			commitChanges();

		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.putAll", timer);
		}
	}

	@Override
	public V remove(final Object key) {
		optimize();
		final long timer = OProfiler.getInstance().startChrono();

		try {
			for (int i = 0; i < OPTIMIZE_MAX_RETRY; ++i) {
				try {

					V v = super.remove(key);
					commitChanges();
					return v;

				} catch (OLowMemoryException e) {
					OLogManager.instance().debug(this, "Optimization required during remove %d/%d", i, OPTIMIZE_MAX_RETRY);

					// LOW MEMORY DURING REMOVAL: THIS MEANS DEEP LOADING OF NODES. EXECUTE THE OPTIMIZATION AND RETRY IT
					optimize(true);

					System.gc();

					if (i > 0)
						// WAIT A PROPORTIONAL TIME
						try {
							Thread.sleep(300 * i);
						} catch (InterruptedException e1) {
						}

					// AVOID CONTINUE EXCEPTIONS
					optimization = -1;
				}
			}
		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.remove", timer);
		}

		throw new OLowMemoryException("OMVRBTreePersistent.remove()");
	}

	public int commitChanges() {
		final long timer = OProfiler.getInstance().startChrono();

		int totalCommitted = 0;
		try {
			if (record.isDirty()) {
				// TREE IS CHANGED AS WELL
				save();
			}

			if (!recordsToCommit.isEmpty()) {
				final List<OMVRBTreeEntryPersistent<K, V>> tmp = new ArrayList<OMVRBTreeEntryPersistent<K, V>>();

				while (recordsToCommit.iterator().hasNext()) {
					// COMMIT BEFORE THE NEW RECORDS (TO ASSURE RID IN RELATIONSHIPS)
					tmp.addAll(recordsToCommit);

					recordsToCommit.clear();

					for (OMVRBTreeEntryPersistent<K, V> node : tmp)
						if (node.record.isDirty()) {
							boolean wasNew = node.record.getIdentity().isNew();

							// CREATE THE RECORD
							node.save();

							if (debug)
								System.out.printf("\nSaved %s tree node %s: parent %s, left %s, right %s", wasNew ? "new" : "",
										node.record.getIdentity(), node.parentRid, node.leftRid, node.rightRid);

							if (wasNew) {
								if (node.record.getIdentity().getClusterPosition() < -1) {
									// TX RECORD
									if (cache.get(node.record.getIdentity()) != node)
										// INSERT A COPY TO PREVENT CHANGES
										cache.put(node.record.getIdentity().copy(), node);
								}

								cache.put(node.record.getIdentity(), node);
							}
						}

					totalCommitted += tmp.size();
					tmp.clear();
				}
			}

		} catch (IOException e) {
			OLogManager.instance().exception("Error on saving the tree", e, OStorageException.class);

		} finally {

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.commitChanges", timer);
		}

		return totalCommitted;
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		final ORID rootRid = new ORecordId();

		try {
			final OMemoryStream stream = new OMemoryStream(iStream);

			byte protocolVersion = stream.peek();
			if (protocolVersion != -1) {
				// @COMPATIBILITY BEFORE 0.9.25
				stream.getAsByte();
				if (protocolVersion != CURRENT_PROTOCOL_VERSION)
					throw new OSerializationException(
							"The index has been created with a previous version of OrientDB. Soft transitions between version is a featured supported since 0.9.25. In order to use it with this version of OrientDB you need to export and import your database. "
									+ protocolVersion + "<->" + CURRENT_PROTOCOL_VERSION);
			}

			rootRid.fromStream(stream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

			size = stream.getAsInteger();
			if (protocolVersion == -1)
				// @COMPATIBILITY BEFORE 0.9.25
				lastPageSize = stream.getAsShort();
			else
				lastPageSize = stream.getAsInteger();

			serializerFromStream(stream);

			// LOAD THE ROOT OBJECT AFTER ALL
			if (rootRid.isValid())
				root = loadEntry(null, rootRid);

			return this;

		} catch (Exception e) {

			OLogManager.instance().error(this, "Error on unmarshalling OMVRBTreePersistent object from record: %s", e,
					OSerializationException.class, rootRid);

		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.fromStream", timer);
		}
		return this;
	}

	public byte[] toStream() throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		final Integer identityRecord = System.identityHashCode(record);
		final Set<Integer> marshalledRecords = OSerializationThreadLocal.INSTANCE.get();
		if (marshalledRecords.contains(identityRecord)) {
			// ALREADY IN STACK, RETURN EMPTY
			return new byte[] {};
		} else
			marshalledRecords.add(identityRecord);

		OMemoryOutputStream stream = entryRecordBuffer;

		try {
			stream.add(CURRENT_PROTOCOL_VERSION);

			if (root != null) {
				OMVRBTreeEntryPersistent<K, V> pRoot = (OMVRBTreeEntryPersistent<K, V>) root;
				if (pRoot.record.getIdentity().isNew()) {
					// FIRST TIME: SAVE IT
					pRoot.save();
				}

				stream.addAsFixed(pRoot.record.getIdentity().toStream());
			} else
				stream.addAsFixed(ORecordId.EMPTY_RECORD_ID_STREAM);

			stream.add(size);
			stream.add(lastPageSize);

			stream.add(keySerializer.getName());
			stream.add(valueSerializer.getName());

			record.fromStream(stream.getByteArray());
			return record.toStream();

		} catch (IOException e) {
			throw new OSerializationException("Error on marshalling RB+Tree", e);
		} finally {
			marshalledRecords.remove(identityRecord);

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.toStream", timer);
		}
	}

	public void signalTreeChanged(final OMVRBTree<K, V> iTree) {
		record.setDirty();
	}

	public void signalNodeChanged(final OMVRBTreeEntry<K, V> iNode) {
		recordsToCommit.add((OMVRBTreeEntryPersistent<K, V>) iNode);
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
	}

	@Override
	public V get(final Object iKey) {
		for (int i = 0; i < OPTIMIZE_MAX_RETRY; ++i) {
			try {
				return super.get(iKey);
			} catch (OLowMemoryException e) {
				OLogManager.instance().debug(this, "Optimization required during load %d/%d", i, OPTIMIZE_MAX_RETRY);

				// LOW MEMORY DURING LOAD: THIS MEANS DEEP LOADING OF NODES. EXECUTE THE OPTIMIZATION AND RETRY IT
				optimize(true);

				System.gc();

				if (i > 0)
					// WAIT A PROPORTIONAL TIME
					try {
						Thread.sleep(300 * i);
					} catch (InterruptedException e1) {
					}
			}
		}

		throw new OLowMemoryException("OMVRBTreePersistent.get()");
	}

	public V get(final Object iKey, final String iFetchPlan) {
		fetchPlan = iFetchPlan;
		return get(iKey);
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

	public int getOptimization() {
		return optimization;
	}

	public void setOptimization(final int i) {
		if (i > 0 && optimization == -1)
			// IGNORE IT, ALREADY RUNNING
			return;

		optimization = i;
	}

	/**
	 * Checks if optimization is needed by raising a {@link OLowMemoryException}.
	 */
	@Override
	protected void searchNodeCallback() {
		if (optimization > 0)
			throw new OLowMemoryException("Optimization level: " + optimization);
	}

	public int getOptimizeThreshold() {
		return optimizeThreshold;
	}

	public void setOptimizeThreshold(int optimizeThreshold) {
		this.optimizeThreshold = optimizeThreshold;
	}

	public int getEntryPointSize() {
		return entryPointsSize;
	}

	public void setEntryPointSize(int entryPointSize) {
		this.entryPointsSize = entryPointSize;
	}

	@Override
	public String toString() {

		final StringBuilder buffer = new StringBuilder();
		buffer.append("size=");

		buffer.append(size);

		if (size > 0) {
			final int currPageIndex = pageIndex;
			buffer.append(" ");
			buffer.append(getFirstEntry().getFirstKey());
			buffer.append("-");
			buffer.append(getLastEntry().getLastKey());
			pageIndex = currPageIndex;
		}

		return buffer.toString();
	}

	private V internalPut(final K key, final V value) throws OLowMemoryException {
		ORecord<?> rec;

		if (key instanceof ORecord<?>) {
			// RECORD KEY: ASSURE IT'S PERSISTENT TO AVOID STORING INVALID RIDs
			rec = (ORecord<?>) key;
			if (!rec.getIdentity().isValid())
				rec.save();
		}

		if (value instanceof ORecord<?>) {
			// RECORD VALUE: ASSURE IT'S PERSISTENT TO AVOID STORING INVALID RIDs
			rec = (ORecord<?>) value;
			if (!rec.getIdentity().isValid())
				rec.save();
		}

		for (int i = 0; i < OPTIMIZE_MAX_RETRY; ++i) {
			try {
				final V previous = super.put(key, value);

				if (optimizeThreshold > -1 && ++insertionCounter >= optimizeThreshold) {
					insertionCounter = 0;
					optimization = 2;
					optimize(true);
				}

				return previous;
			} catch (OLowMemoryException e) {
				OLogManager.instance().debug(this, "Optimization required during put %d/%d", i, OPTIMIZE_MAX_RETRY);

				// LOW MEMORY DURING PUT: THIS MEANS DEEP LOADING OF NODES. EXECUTE THE OPTIMIZATION AND RETRY IT
				optimize(true);

				System.gc();

				if (i > 0)
					// WAIT A PROPORTIONAL TIME
					try {
						Thread.sleep(300 * i);
					} catch (InterruptedException e1) {
					}
			}
		}

		throw new OLowMemoryException("OMVRBTreePersistent.put()");
	}

	public Object getKeySerializer() {
		return keySerializer;
	}

	public Object getValueSerializer() {
		return valueSerializer;
	}

	/**
	 * Returns the best entry point to start the search. Searches first between entrypoints. If nothing is found "root" is always
	 * returned.
	 */
	@Override
	protected OMVRBTreeEntry<K, V> getBestEntryPoint(final K iKey) {
		if (!entryPoints.isEmpty()) {
			// SEARCHES EXACT OR BIGGER ENTRY
			Entry<K, OMVRBTreeEntryPersistent<K, V>> closerNode = entryPoints.floorEntry(iKey);
			if (closerNode != null)
				return closerNode.getValue();

			// NO WAY: TRY WITH ANY NODE BEFORE THE KEY
			closerNode = entryPoints.ceilingEntry(iKey);
			if (closerNode != null)
				return closerNode.getValue();
		}

		// USE ROOT
		return super.getBestEntryPoint(iKey);
	}

	/**
	 * Remove an entry point from the list
	 */
	void removeEntryPoint(final OMVRBTreeEntryPersistent<K, V> iEntry) {
		entryPoints.remove(iEntry);
	}

	synchronized void removeEntry(final ORID iEntryId) {
		// DELETE THE NODE FROM THE PENDING RECORDS TO COMMIT
		for (OMVRBTreeEntryPersistent<K, V> node : recordsToCommit) {
			if (node.record.getIdentity().equals(iEntryId)) {
				recordsToCommit.remove(node);
				break;
			}
		}
	}

	/**
	 * Returns the first Entry in the OMVRBTree (according to the OMVRBTree's key-sort function). Returns null if the OMVRBTree is
	 * empty.
	 */
	@Override
	protected OMVRBTreeEntry<K, V> getFirstEntry() {
		if (!entryPoints.isEmpty()) {
			// FIND THE FIRST ELEMENT STARTING FROM THE FIRST ENTRY-POINT IN MEMORY
			final Map.Entry<K, OMVRBTreeEntryPersistent<K, V>> entry = entryPoints.firstEntry();

			if (entry != null) {
				OMVRBTreeEntryPersistent<K, V> e = entry.getValue();

				OMVRBTreeEntryPersistent<K, V> prev;
				do {
					prev = (OMVRBTreeEntryPersistent<K, V>) predecessor(e);
					if (prev != null)
						e = prev;
				} while (prev != null);

				if (e != null && e.getSize() > 0)
					pageIndex = 0;

				return e;
			}
		}

		// SEARCH FROM ROOT
		return super.getFirstEntry();
	}

	/**
	 * Returns the last Entry in the OMVRBTree (according to the OMVRBTree's key-sort function). Returns null if the OMVRBTree is
	 * empty.
	 */
	@Override
	protected OMVRBTreeEntry<K, V> getLastEntry() {
		if (!entryPoints.isEmpty()) {
			// FIND THE LAST ELEMENT STARTING FROM THE FIRST ENTRY-POINT IN MEMORY
			final Map.Entry<K, OMVRBTreeEntryPersistent<K, V>> entry = entryPoints.lastEntry();

			if (entry != null) {
				OMVRBTreeEntryPersistent<K, V> e = entry.getValue();

				OMVRBTreeEntryPersistent<K, V> next;
				do {
					next = (OMVRBTreeEntryPersistent<K, V>) successor(e);
					if (next != null)
						e = next;
				} while (next != null);

				if (e != null && e.getSize() > 0)
					pageIndex = e.getSize() - 1;

				return e;
			}
		}

		// SEARCH FROM ROOT
		return super.getLastEntry();
	}

	@Override
	protected void setRoot(final OMVRBTreeEntry<K, V> iRoot) {
		if (iRoot == root)
			return;

		super.setRoot(iRoot);
		if (listener != null)
			listener.signalTreeChanged(this);
	}

	protected void config() {
		lastPageSize = OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.getValueAsInteger();
		pageLoadFactor = OGlobalConfiguration.MVRBTREE_LOAD_FACTOR.getValueAsFloat();
		optimizeEntryPointsFactor = OGlobalConfiguration.MVRBTREE_OPTIMIZE_ENTRYPOINTS_FACTOR.getValueAsFloat();
		optimizeThreshold = OGlobalConfiguration.MVRBTREE_OPTIMIZE_THRESHOLD.getValueAsInteger();
		entryPointsSize = OGlobalConfiguration.MVRBTREE_ENTRYPOINTS.getValueAsInteger();
	}

	protected void serializerFromStream(final OMemoryStream stream) throws IOException {
		keySerializer = OStreamSerializerFactory.get(stream.getAsString());
		valueSerializer = OStreamSerializerFactory.get(stream.getAsString());
	}

	@Override
	protected void rotateLeft(final OMVRBTreeEntry<K, V> p) {
		if (debug && p != null)
			System.out.printf("\nRotating to the left the node %s", ((OMVRBTreeEntryPersistent<K, V>) p).record.getIdentity());
		super.rotateLeft(p);
	}

	@Override
	protected void rotateRight(final OMVRBTreeEntry<K, V> p) {
		if (debug && p != null)
			System.out.printf("\nRotating to the right the node %s", ((OMVRBTreeEntryPersistent<K, V>) p).record.getIdentity());
		super.rotateRight(p);
	}

	protected ODatabaseRecord getDatabase() {
		final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();
		record.setDatabase(database);
		return database;
	}

	/**
	 * Removes the node also from the memory.
	 */
	@Override
	protected void removeNode(final OMVRBTreeEntry<K, V> p) {
		removeNodeFromMemory((OMVRBTreeEntryPersistent<K, V>) p);
		super.removeNode(p);
	}

	/**
	 * Removes the node from the memory.
	 * 
	 * @param iNode
	 *          Node to remove
	 */
	protected void removeNodeFromMemory(final OMVRBTreeEntryPersistent<K, V> iNode) {
		if (iNode.record != null)
			cache.remove(iNode.record.getIdentity());
		if (iNode.getSize() > 0)
			entryPoints.remove(iNode.getKeyAt(0));
	}

	protected boolean isNodeEntryPoint(final OMVRBTreeEntryPersistent<K, V> iNode) {
		if (iNode != null && iNode.getSize() > 0)
			return entryPoints.containsKey(iNode.getKeyAt(0));
		return false;
	}

	protected void addNodeAsEntrypoint(final OMVRBTreeEntryPersistent<K, V> iNode) {
		if (iNode != null && iNode.getSize() > 0)
			entryPoints.put(iNode.getKeyAt(0), iNode);
	}

	/**
	 * Updates the position of the node between the entry-points. If the node has 0 items, it's simply removed.
	 * 
	 * @param iOldKey
	 *          Old key to remove
	 * @param iNode
	 *          Node to update
	 */
	protected void updateEntryPoint(final K iOldKey, final OMVRBTreeEntryPersistent<K, V> iNode) {
		final OMVRBTreeEntryPersistent<K, V> node = entryPoints.remove(iOldKey);
		if (node != iNode)
			OLogManager.instance().warn(this, "Entrypoints nodes are different during update: old %s <-> new %s", node, iNode);
		addNodeAsEntrypoint(node);
	}

	/**
	 * Keeps the node in memory.
	 * 
	 * @param iNode
	 *          Node to store
	 */
	protected void addNodeInCache(final OMVRBTreeEntryPersistent<K, V> iNode) {
		if (iNode.record != null)
			cache.put(iNode.record.getIdentity(), iNode);
	}

	/**
	 * Searches the node in local cache by RID.
	 * 
	 * @param iRid
	 *          RID to search
	 * @return Node is found, otherwise NULL
	 */
	protected OMVRBTreeEntryPersistent<K, V> searchNodeInCache(final ORID iRid) {
		return cache.get(iRid);
	}

	public int getNumberOfNodesInCache() {
		return cache.size();
	}

	/**
	 * Returns all the RID of the nodes in memory.
	 */
	protected Set<ORID> getAllNodesInCache() {
		return cache.keySet();
	}

	/**
	 * Removes the node from the local cache.
	 * 
	 * @param iNode
	 *          Node to remove
	 */
	protected void removeNodeFromCache(final ORID iRid) {
		cache.remove(iRid);
	}
}
