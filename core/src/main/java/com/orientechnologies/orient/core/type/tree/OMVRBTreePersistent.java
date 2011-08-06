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
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryOutputStream;
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
	protected OStreamSerializer													keySerializer;
	protected OStreamSerializer													valueSerializer;

	protected final Set<OMVRBTreeEntryPersistent<K, V>>	recordsToCommit						= new HashSet<OMVRBTreeEntryPersistent<K, V>>();

	protected final String															clusterName;
	protected ORecordBytesLazy													record;
	protected String																		fetchPlan;

	// STORES IN MEMORY DIRECT REFERENCES TO PORTION OF THE TREE
	protected int																				optimizeThreshold;
	private int																					insertionCounter					= 0;
	protected int																				entryPointsSize;
	protected float																			optimizeEntryPointsFactor;
	protected List<OMVRBTreeEntryPersistent<K, V>>			entryPoints								= new ArrayList<OMVRBTreeEntryPersistent<K, V>>(
																																										entryPointsSize);

	protected Map<ORID, OMVRBTreeEntryPersistent<K, V>>	cache											= new HashMap<ORID, OMVRBTreeEntryPersistent<K, V>>();
	private final OMemoryOutputStream										entryRecordBuffer;
	public final static byte														CURRENT_PROTOCOL_VERSION	= 0;

	public OMVRBTreePersistent(final String iClusterName, final ORID iRID) {
		this(iClusterName, null, null);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OMVRBTreePersistent(String iClusterName, final OStreamSerializer iKeySerializer, final OStreamSerializer iValueSerializer) {
		// MINIMIZE I/O USING A LARGER PAGE THAN THE DEFAULT USED IN MEMORY
		super(OGlobalConfiguration.MVRBTREE_NODE_PAGE_SIZE.getValueAsInteger(), 0.7f);
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
	protected abstract OMVRBTreeEntryPersistent<K, V> loadEntry(OMVRBTreeEntryPersistent<K, V> iParent, ORID iRecordId)
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
			for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints)
				entryPoint.disconnectLinked(true);
			entryPoints.clear();

			recordsToCommit.clear();
			cache.clear();
			root = null;

			load();

		} catch (IOException e) {
			OLogManager.instance().error(this, "Error on unload the tree: " + record.getIdentity(), e, OStorageException.class);
		} finally {

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.unload", timer);
		}
	}

	/**
	 * Optimize the tree memory consumption by keeping part of nodes as entry points and clearing all the rest.
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public int optimize(final boolean iForce) {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			if (root == null)
				return 0;

			OLogManager.instance().debug(this, "Starting optimization of MVRB+Tree with %d items in memory...", cache.size());

			// printInMemoryStructure();

			OMVRBTreeEntryPersistent<K, V> pRoot = (OMVRBTreeEntryPersistent<K, V>) root;

			if (entryPoints.size() == 0)
				// FIRST TIME THE LIST IS NULL: START FROM ROOT
				entryPoints.add(pRoot);

			// RECONFIG IT TO CATCH CHANGED VALUES
			config();

			int nodes = 0;
			List<OMVRBTreeEntryPersistent<K, V>> tmp = null;

			if (isRuntimeCheckEnabled())
				tmp = new ArrayList<OMVRBTreeEntryPersistent<K, V>>();

			for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints) {
				for (OMVRBTreeEntryPersistent<K, V> e = (OMVRBTreeEntryPersistent<K, V>) entryPoint.getFirstInMemory(); e != null; e = e
						.getNextInMemory()) {

					if (isRuntimeCheckEnabled()) {
						for (OMVRBTreeEntryPersistent<K, V> t : tmp)
							if (t != e && t.record.getIdentity().equals(e.record.getIdentity())) {
								OLogManager.instance().error(this, "Found Node loaded in memory twice with different instances: " + e);
								continue;
							}

						tmp.add(e);
					}

					++nodes;
				}
			}

			if (OLogManager.instance().isDebugEnabled())
				OLogManager.instance().debug(this, "Found %d nodes in memory, %d items on disk, threshold=%d, entryPoints=%d", nodes, size,
						(entryPointsSize * optimizeEntryPointsFactor), entryPoints.size());

			if (!iForce && nodes < entryPointsSize * optimizeEntryPointsFactor)
				// UNDER THRESHOLD AVOID TO OPTIMIZE
				return 0;

			if (debug)
				System.out.printf("\n------------\nOptimizing: total items %d, root is %s...", size(), pRoot.toString());

			lastSearchFound = false;
			lastSearchKey = null;
			lastSearchNode = null;

			// COMPUTE THE DISTANCE BETWEEN NODES
			final int distance;
			if (nodes <= entryPointsSize)
				distance = 1;
			else
				distance = nodes / entryPointsSize + 1;

			final List<OMVRBTreeEntryPersistent<K, V>> newEntryPoints = new ArrayList<OMVRBTreeEntryPersistent<K, V>>(entryPointsSize + 1);

			OLogManager.instance().debug(this, "Compacting nodes with distance = %d", distance);

			// PICK NEW ENTRYPOINTS AT EQUAL DISTANCE
			int nodeCounter = 0;
			OMVRBTreeEntryPersistent<K, V> lastNode = null;
			OMVRBTreeEntryPersistent<K, V> currNode;

			for (int i = 0; i < entryPoints.size(); ++i) {
				currNode = entryPoints.get(i);
				for (OMVRBTreeEntryPersistent<K, V> e = (OMVRBTreeEntryPersistent<K, V>) currNode.getFirstInMemory(); e != null; e = e
						.getNextInMemory()) {

					boolean alreadyPresent = false;

					// CHECK THAT THE NODE IS NOT PART OF A NEXT ENTRY-POINTS: THIS IS THE CASE WHEN THE TREE CHUNKS ARE CONNECTED
					// BETWEEN THEM
					for (int k = i + 1; k < entryPoints.size(); ++k)
						if (e == entryPoints.get(k)) {
							alreadyPresent = true;
							break;
						}

					if (alreadyPresent)
						continue;

					++nodeCounter;

					if (newEntryPoints.size() == 0 || nodeCounter % distance == 0) {
						for (OMVRBTreeEntryPersistent<K, V> ep : newEntryPoints)
							if (ep == e) {
								alreadyPresent = true;
								break;
							}

						if (alreadyPresent)
							--nodeCounter;
						else
							newEntryPoints.add(e);
					}

					lastNode = e;
				}
			}

			if (newEntryPoints.size() > 1 && newEntryPoints.get(newEntryPoints.size() - 1) != lastNode)
				// ADD THE LAST ONE IF IT'S NOT YET PRESENT
				newEntryPoints.add(lastNode);

			// INSERT ROOT BETWEEN ENTRY-POINTS
			int cmp;
			for (int i = 0; i < newEntryPoints.size(); ++i) {
				cmp = ((Comparable<K>) pRoot.getFirstKey()).compareTo(newEntryPoints.get(i).getFirstKey());
				if (cmp < 0) {
					newEntryPoints.add(i, pRoot);
					break;
				} else if (cmp == 0)
					// ALREADY PRESENT: DO NOTHING
					break;
			}

			// REMOVE NOT REFERENCED ENTRY POINTS
			for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints) {
				if (entryPoint.parent == null && entryPoint.left == null && entryPoint.right == null && entryPoint != root) {
					// CHECK IF IT'S NOT PART OF NEW ENTRYPOINTS
					boolean found = false;
					for (OMVRBTreeEntryPersistent<K, V> e : newEntryPoints) {
						if (e == entryPoint) {
							// IT'S AN ENTRYPOINT
							found = true;
							break;
						}
					}

					if (!found) {
						// NODE NOT REFERENCED: REMOVE IT FROM THE CACHE
						cache.remove(entryPoint.record.getIdentity());
						entryPoint.clear();
					}
				}
			}

			final List<OMVRBTreeEntryPersistent<K, V>> oldEntryPoints = entryPoints;
			entryPoints = newEntryPoints;

			// FREE ALL THE NODES READING THE OLD ENTRY POINTS BUT THE NEW ENTRY POINTS
			int totalDisconnected = 0;
			for (OMVRBTreeEntryPersistent<K, V> entryPoint : oldEntryPoints) {
				totalDisconnected += entryPoint.disconnectLinked(false);
			}

			oldEntryPoints.clear();

			if (isRuntimeCheckEnabled()) {
				for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints)
					for (OMVRBTreeEntryPersistent<K, V> e = (OMVRBTreeEntryPersistent<K, V>) entryPoint.getFirstInMemory(); e != null; e = e
							.getNextInMemory())
						e.checkEntryStructure();

				if (OLogManager.instance().isDebugEnabled()) {
					// COUNT ALL IN-MEMORY NODES BY BROWSING ALL THE ENTRYPOINT NODES
					nodes = 0;
					for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints)
						for (OMVRBTreeEntryPersistent<K, V> e = (OMVRBTreeEntryPersistent<K, V>) entryPoint.getFirstInMemory(); e != null; e = e
								.getNextInMemory())
							++nodes;

					OLogManager.instance().debug(this, "Now Found %d nodes in memory and threshold=%d. EntryPoints=%d", nodes,
							(entryPointsSize * optimizeEntryPointsFactor), entryPoints.size());
				}
			}

			OLogManager.instance().debug(this, "Optimization done: MVRB-Tree nodes reduced to %d items", cache.size());

			return totalDisconnected;
		} finally {
			// System.out.println("End of optimization.");
			// printInMemoryStructure();

			if (isRuntimeCheckEnabled()) {
				if (entryPoints.size() > 0)
					for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints)
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
		final long timer = OProfiler.getInstance().startChrono();

		try {
			V v = super.remove(key);
			commitChanges();
			return v;
		} finally {

			OProfiler.getInstance().stopChrono("OMVRBTreePersistent.remove", timer);
		}
	}

	public int commitChanges() {
		final long timer = OProfiler.getInstance().startChrono();

		int totalCommitted = 0;
		try {
			if (record.isDirty()) {
				// TREE IS CHANGED AS WELL
				save();
			}

			if (recordsToCommit.size() > 0) {
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
									if (cache.get(node.record) != node)
										// INSERT A COPY TO PREVENT CHANGES
										cache.put(node.record.getIdentity().copy(), node);
								} else {
									// NEW RID: MAKE DIRTY THE LINKED NODES
									if (node.parent != null)
										(node.parent).markDirty();
									if (node.left != null)
										(node.left).markDirty();
									if (node.right != null)
										(node.right).markDirty();
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
			final OMemoryInputStream stream = new OMemoryInputStream(iStream);

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

	private V internalPut(final K key, final V value) {
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

		final V previous = super.put(key, value);

		if (optimizeThreshold > -1 && insertionCounter > optimizeThreshold) {
			insertionCounter = 0;
			optimize(false);
		} else
			++insertionCounter;

		return previous;
	}

	public Object getKeySerializer() {
		return keySerializer;
	}

	public Object getValueSerializer() {
		return valueSerializer;
	}

	/**
	 * Returns the best entry point to start the search.
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected OMVRBTreeEntry<K, V> getBestEntryPoint(final Object iKey) {
		final Comparable<? super K> key = (Comparable<? super K>) iKey;

		OMVRBTreeEntryPersistent<K, V> bestNode = null;
		int entryPointSize = entryPoints.size();

		if (entryPointSize == 0)
			// TREE EMPTY: RETURN ROOT
			return root;

		// CLAN EMPTY NODES
		for (Iterator<OMVRBTreeEntryPersistent<K, V>> iter = entryPoints.iterator(); iter.hasNext();) {
			if (iter.next().getSize() == 0) {
				iter.remove();
				--entryPointSize;
			}
		}

		// 2^ CHANCE - TRY TO SEE IF LAST ENTRYPOINT IS GOOD: THIS IS VERY COMMON CASE ON INSERTION WITH AN INCREMENTING KEY
		OMVRBTreeEntryPersistent<K, V> e = entryPoints.get(entryPointSize - 1);
		int cmp = key.compareTo(e.getFirstKey());
		if (cmp <= 0)
			return e;

		// SEARCH THE CLOSEST KEY
		if (entryPointSize < OMVRBTreeEntry.BINARY_SEARCH_THRESHOLD) {
			// LINEAR SEARCH
			for (int i = 0; i < entryPointSize; ++i) {
				e = entryPoints.get(i);

				if (e.serializedKeys == null) {
					// CLEAN WRONG ENTRY (WHY THEY ARE WRONG?)
					OLogManager.instance().error(this, "Found wrong entrypoint in position %d", i);
					entryPoints.remove(i);
					--i;
					continue;
				}

				cmp = key.compareTo(e.getFirstKey());
				if (cmp < 0) {
					// RETURN THE PREVIOUS ONE OF CURRENT IF IT'S NULL
					return bestNode != null ? bestNode : e;
				} else if (cmp >= 0 && key.compareTo(e.getLastKey()) <= 0)
					// PERFECT MATCH, VERY LUCKY: RETURN THE CURRENT = 0 READS
					return e;

				// SET THE CURRENT AS BEST NODE
				bestNode = e;
			}
		} else {
			// BINARY SEARCH
			int low = 0;
			int high = entryPointSize - 1;
			int mid = 0;

			while (low <= high) {
				mid = (low + high) >>> 1;
				e = entryPoints.get(mid);

				if (e.serializedKeys == null) {
					// CLEAN WRONG ENTRY (WHY THEY ARE WRONG?)
					OLogManager.instance().error(this, "Found wrong entrypoint in position %d", mid);
					entryPoints.remove(mid);
					low = 0;
					entryPointSize = entryPoints.size();
					high = entryPointSize - 1;
					continue;
				}

				cmp = key.compareTo(e.getFirstKey());

				if (cmp >= 0 && key.compareTo(e.getLastKey()) <= 0)
					// PERFECT MATCH, VERY LUCKY: RETURN THE CURRENT = 0 READS
					return e;

				if (low == high)
					break;

				if (cmp > 0)
					low = mid + 1;
				else
					high = mid;

				// SET THE CURRENT AS BEST NODE
				bestNode = e;
			}

			if (mid > 0 && key.compareTo(bestNode.getFirstKey()) < 0)
				// GET THE PREVIOUS ONE
				bestNode = entryPoints.get(mid - 1);
		}

		// RETURN THE LATEST ONE
		return bestNode;
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
		if (entryPoints.size() > 0) {
			// FIND THE FIRST ELEMENT STARTING FROM THE FIRST NODE
			OMVRBTreeEntryPersistent<K, V> e = entryPoints.get(0);

			while (e.getLeft() != null) {
				e = (OMVRBTreeEntryPersistent<K, V>) e.getLeft();
			}
			return e;
		}

		return super.getFirstEntry();
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

	protected void serializerFromStream(final OMemoryInputStream stream) throws IOException {
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

	protected boolean isEntryPoint(final OMVRBTreeEntry<K, V> iEntry) {
		for (OMVRBTreeEntryPersistent<K, V> entryPoint : entryPoints) {
			if (entryPoint == iEntry) {
				// IT'S AN ENTRYPOINT
				return true;
			}
		}
		return false;
	}

	protected void removeNode(final OMVRBTreeEntry<K, V> p) {
		entryPoints.remove(p);
		super.removeNode(p);
	}

}
