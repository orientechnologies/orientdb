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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.ONavigableMap;
import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerLiteral;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabaseLazySave;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexMVRBTreeAbstract extends OSharedResourceExternal implements OIndexInternal, ODatabaseListener {
	protected static final String																		CONFIG_MAP_RID	= "mapRid";
	protected static final String																		CONFIG_CLUSTERS	= "clusters";
	protected String																								name;
	protected String																								type;
	protected OMVRBTreeDatabaseLazySave<Object, Set<OIdentifiable>>	map;
	protected Set<String>																						clustersToIndex	= new LinkedHashSet<String>();
	protected OIndexCallback																				callback;
	protected boolean																								automatic;
	protected OType																									keyType;

	@ODocumentInstance
	protected ODocument																							configuration;
	private Listener																								watchDog;
	private volatile int																						optimization		= 0;

	public OIndexMVRBTreeAbstract(final String iType) {
		type = iType;

		watchDog = new Listener() {
			public void memoryUsageLow(final TYPE iType, final long usedMemory, final long maxMemory) {
				if (iType == TYPE.JVM)
					optimization = 1;
			}

			public void memoryUsageCritical(final TYPE iType, final long usedMemory, final long maxMemory) {
				if (iType == TYPE.JVM)
					optimization = 2;
			}
		};
	}

	public void flush() {
		lazySave();
	}

	/**
	 * Creates the index.
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 * @param iClusterIndexName
	 *          Cluster name where to place the TreeMap
	 * @param iProgressListener
	 *          Listener to get called on progress
	 */
	public OIndexInternal create(final String iName, final OType iKeyType, final ODatabaseRecord iDatabase,
			final String iClusterIndexName, final int[] iClusterIdsToIndex, final OProgressListener iProgressListener,
			final boolean iAutomatic) {
		acquireExclusiveLock();
		try {

			name = iName;
			configuration = new ODocument(iDatabase);
			automatic = iAutomatic;
			keyType = iKeyType;

			if (iClusterIdsToIndex != null)
				for (int id : iClusterIdsToIndex)
					clustersToIndex.add(iDatabase.getClusterNameById(id));

			map = new OMVRBTreeDatabaseLazySave<Object, Set<OIdentifiable>>(iDatabase, iClusterIndexName,
					OStreamSerializerLiteral.INSTANCE, OStreamSerializerListRID.INSTANCE);

			installHooks(iDatabase);

			rebuild(iProgressListener);
			updateConfiguration();
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexInternal loadFromConfiguration(final ODocument iConfig) {
		acquireExclusiveLock();
		try {

			final ORID rid = (ORID) iConfig.field(CONFIG_MAP_RID, ORID.class);
			if (rid == null)
				return null;

			configuration = iConfig;
			name = configuration.field(OIndexInternal.CONFIG_NAME);
			automatic = (Boolean) (configuration.field(OIndexInternal.CONFIG_AUTOMATIC) != null ? configuration
					.field(OIndexInternal.CONFIG_AUTOMATIC) : true);

			final String configuredKeyType = configuration.field(OIndexInternal.CONFIG_KEYTYPE);
			if (configuredKeyType != null)
				keyType = OType.valueOf(configuredKeyType.toUpperCase());

			clustersToIndex.clear();

			final Collection<? extends String> clusters = configuration.field(CONFIG_CLUSTERS);
			if (clusters != null)
				clustersToIndex.addAll(clusters);

			map = new OMVRBTreeDatabaseLazySave<Object, Set<OIdentifiable>>(getDatabase(), rid);
			map.load();

			installHooks(iConfig.getDatabase());

			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public Set<OIdentifiable> get(final Object iKey) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			final ORecordLazySet values = (ORecordLazySet) map.get(iKey);
			if (values != null)
				values.setDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());

			if (values == null)
				return ORecordLazySet.EMPTY_SET;

			return values;

		} finally {
			releaseExclusiveLock();
		}
	}

	public boolean contains(final Object iKey) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			return map.containsKey(iKey);

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Returns a set of records with key between the range passed as parameter. Range bounds are included.
	 * 
	 * @param iRangeFrom
	 *          Starting range
	 * @param iRangeTo
	 *          Ending range
	 * @see #getValuesBetween(Object, Object, boolean)
	 * @return
	 */
	public Set<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo) {
		return getValuesBetween(iRangeFrom, iRangeTo, true);
	}

	/**
	 * Returns a set of records with keys greater than passed parameter.
	 * 
	 * @param fromKey
	 *          Starting key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with keys greater than passed parameter.
	 */
	public Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.tailMap(fromKey, isInclusive);
			if (subSet == null)
				return ORecordLazySet.EMPTY_SET;

			final Set<OIdentifiable> result = new ORecordLazySet(configuration.getDatabase());
			for (Set<OIdentifiable> v : subSet.values()) {
				result.addAll(v);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
	 * greater than passed parameter.
	 * 
	 * @param fromKey
	 *          Starting key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with key greater than passed parameter.
	 */
	public Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.tailMap(fromKey, isInclusive);
			if (subSet != null) {
				for (Entry<Object, Set<OIdentifiable>> v : subSet.entrySet()) {
					for (OIdentifiable id : v.getValue()) {
						final ODocument document = new ODocument();
						document.field("key", v.getKey());
						document.field("rid", id.getIdentity());
						document.unsetDirty();
						result.add(document);
					}
				}
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Returns a set of records with keys less than passed parameter.
	 * 
	 * @param toKey
	 *          Ending key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with keys less than passed parameter.
	 */
	public Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.headMap(toKey, isInclusive);
			if (subSet == null)
				return ORecordLazySet.EMPTY_SET;

			final Set<OIdentifiable> result = new ORecordLazySet(configuration.getDatabase());
			for (Set<OIdentifiable> v : subSet.values()) {
				result.addAll(v);
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
	 * less than passed parameter.
	 * 
	 * @param toKey
	 *          Ending key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with key greater than passed parameter.
	 */
	public Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive) {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.headMap(toKey, isInclusive);
			if (subSet != null) {
				for (Entry<Object, Set<OIdentifiable>> v : subSet.entrySet()) {
					for (OIdentifiable id : v.getValue()) {
						final ODocument document = new ODocument();
						document.field("key", v.getKey());
						document.field("rid", id.getIdentity());
						document.unsetDirty();
						result.add(document);
					}
				}
			}

			return result;
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Returns a set of records with key between the range passed as parameter.
	 * 
	 * @param iRangeFrom
	 *          Starting range
	 * @param iRangeTo
	 *          Ending range
	 * @param iInclusive
	 *          Include from/to bounds
	 * @see #getValuesBetween(Object, Object)
	 * @return
	 */
	public Set<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		checkForOptimization();
		acquireExclusiveLock();

		try {
			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.subMap(iRangeFrom, iInclusive, iRangeTo, iInclusive);
			if (subSet == null)
				return ORecordLazySet.EMPTY_SET;

			final Set<OIdentifiable> result = new ORecordLazySet(configuration.getDatabase());
			for (Set<OIdentifiable> v : subSet.values()) {
				result.addAll(v);
			}

			return result;

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Returns a set of documents with key between the range passed as parameter. Range bounds are included.
	 * 
	 * @param iRangeFrom
	 *          Starting range
	 * @param iRangeTo
	 *          Ending range
	 * @see #getEntriesBetween(Object, Object, boolean)
	 * @return
	 */
	public Set<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
		return getEntriesBetween(iRangeFrom, iRangeTo, true);
	}

	/**
	 * Returns a set of documents with key between the range passed as parameter.
	 * 
	 * @param iRangeFrom
	 *          Starting range
	 * @param iRangeTo
	 *          Ending range
	 * @param iInclusive
	 *          Include from/to bounds
	 * @see #getEntriesBetween(Object, Object)
	 * @return
	 */
	public Set<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive) {
		if (iRangeFrom.getClass() != iRangeTo.getClass())
			throw new IllegalArgumentException("Range from-to parameters are of different types");

		checkForOptimization();
		acquireExclusiveLock();

		try {
			final Set<ODocument> result = new HashSet<ODocument>();

			final ONavigableMap<Object, Set<OIdentifiable>> subSet = map.subMap(iRangeFrom, iInclusive, iRangeTo, iInclusive);
			if (subSet != null) {
				for (Entry<Object, Set<OIdentifiable>> v : subSet.entrySet()) {
					for (OIdentifiable id : v.getValue()) {
						final ODocument document = new ODocument();
						document.field("key", v.getKey());
						document.field("rid", id.getIdentity());
						document.unsetDirty();
						result.add(document);
					}
				}
			}

			return result;

		} finally {
			releaseExclusiveLock();
		}
	}

	public ORID getIdentity() {
		return map.getRecord().getIdentity();
	}

	public long rebuild() {
		return rebuild(null);
	}

	/**
	 * Populates the index with all the existent records. Uses the massive insert intent to speed up and keep the consumed memory low.
	 */
	public long rebuild(final OProgressListener iProgressListener) {
		clear();

		long documentIndexed = 0;

		final boolean intentInstalled = getDatabase().declareIntent(new OIntentMassiveInsert());

		checkForOptimization();
		acquireExclusiveLock();
		try {

			int documentNum = 0;
			long documentTotal = 0;

			for (String cluster : clustersToIndex)
				documentTotal += getDatabase().countClusterElements(cluster);

			if (iProgressListener != null)
				iProgressListener.onBegin(this, documentTotal);

			for (String clusterName : clustersToIndex)
				for (ORecord<?> record : getDatabase().browseCluster(clusterName)) {
					if (record instanceof ODocument) {
						final ODocument doc = (ODocument) record;
						final Object fieldValue = callback.getDocumentValueToIndex(doc);

						if (fieldValue != null) {
							put(fieldValue, doc);
							++documentIndexed;
						}
					}
					documentNum++;

					if (iProgressListener != null)
						iProgressListener.onProgress(this, documentNum, documentNum * 100f / documentTotal);
				}

			lazySave();

			if (iProgressListener != null)
				iProgressListener.onCompletition(this, true);

		} catch (Exception e) {
			if (iProgressListener != null)
				iProgressListener.onCompletition(this, false);

			clear();

			throw new OIndexException("Error on rebuilding the index for clusters: " + clustersToIndex, e);

		} finally {
			if (intentInstalled)
				getDatabase().declareIntent(null);

			releaseExclusiveLock();
		}

		return documentIndexed;
	}

	public boolean remove(final Object iKey, final OIdentifiable iValue) {
		return remove(iKey);
	}

	public boolean remove(final Object key) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			return map.remove(key) != null;

		} finally {
			releaseExclusiveLock();
		}
	}

	public int remove(final OIdentifiable iRecord) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			int tot = 0;
			Set<OIdentifiable> rids;
			for (Entry<Object, Set<OIdentifiable>> entries : map.entrySet()) {
				rids = entries.getValue();
				if (rids != null) {
					if (rids.contains(iRecord)) {
						remove(entries.getKey(), iRecord);
						++tot;
					}
				}
			}

			return tot;

		} finally {
			releaseExclusiveLock();
		}

	}

	public int count(final OIdentifiable iRecord) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			Set<OIdentifiable> rids;
			int tot = 0;
			for (Entry<Object, Set<OIdentifiable>> entries : map.entrySet()) {
				rids = entries.getValue();
				if (rids != null) {
					if (rids.contains(iRecord)) {
						++tot;
					}
				}
			}

			return tot;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndex clear() {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			map.clear();
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexInternal delete() {
		checkForOptimization();
		acquireExclusiveLock();

		try {
			map.delete();
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexInternal lazySave() {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			map.lazySave();
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public ORecordBytes getRecord() {
		return map.getRecord();
	}

	public Iterator<Entry<Object, Set<OIdentifiable>>> iterator() {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			return map.entrySet().iterator();

		} finally {
			releaseExclusiveLock();
		}
	}

	public Iterable<Object> keys() {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			return map.keySet();

		} finally {
			releaseExclusiveLock();
		}
	}

	public long getSize() {
		checkForOptimization();
		acquireSharedLock();
		try {

			return map.size();

		} finally {
			releaseSharedLock();
		}
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	@Override
	public String toString() {
		return name + " (" + (type != null ? type : "?") + ")" + (map != null ? " " + map : "");
	}

	public OIndexInternal getInternal() {
		return this;
	}

	public OIndexCallback getCallback() {
		return callback;
	}

	public void setCallback(final OIndexCallback callback) {
		this.callback = callback;
	}

	public Set<String> getClusters() {
		checkForOptimization();
		acquireSharedLock();
		try {

			return Collections.unmodifiableSet(clustersToIndex);

		} finally {
			releaseSharedLock();
		}
	}

	public OIndexMVRBTreeAbstract addCluster(final String iClusterName) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			clustersToIndex.add(iClusterName);
			return this;

		} finally {
			releaseSharedLock();
		}
	}

	public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
	}

	public void unload() {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			map.unload();

		} finally {
			releaseExclusiveLock();
		}
	}

	public ODocument updateConfiguration() {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			configuration.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

			try {
				configuration.field(OIndexInternal.CONFIG_TYPE, type);
				configuration.field(OIndexInternal.CONFIG_NAME, name);
				configuration.field(OIndexInternal.CONFIG_AUTOMATIC, automatic);
				if (keyType != null)
					configuration.field(OIndexInternal.CONFIG_KEYTYPE, keyType.toString());
				configuration.field(CONFIG_CLUSTERS, clustersToIndex, OType.EMBEDDEDSET);
				configuration.field(CONFIG_MAP_RID, map.getRecord().getIdentity());

			} finally {
				configuration.setInternalStatus(ORecordElement.STATUS.LOADED);
			}

		} finally {
			releaseExclusiveLock();
		}
		return configuration;
	}

	@SuppressWarnings("unchecked")
	public void commit(final ODocument iDocument) {
		if (iDocument == null)
			return;

		checkForOptimization();
		acquireExclusiveLock();
		try {
			map.setRunningTransaction(true);
			
			final Boolean clearAll = (Boolean) iDocument.field("clear");
			if (clearAll != null && clearAll)
				clear();

			final Collection<ODocument> entries = iDocument.field("entries");

			for (ODocument entry : entries) {
				final Object key = ORecordSerializerStringAbstract.getTypeValue(OStringSerializerHelper.decode((String) entry.field("k")));

				final List<ODocument> operations = (List<ODocument>) entry.field("ops");
				if (operations != null) {
					for (ODocument op : operations) {
						final int operation = (Integer) op.rawField("o");
						final OIdentifiable value = op.field("v");

						if (operation == OPERATION.PUT.ordinal())
							put(key, value);
						else if (operation == OPERATION.REMOVE.ordinal()) {
							if (key.equals("*"))
								remove(value);
							else if (value == null)
								remove(key);
							else
								remove(key, value);
						}
					}
				}
			}

		} finally {
			releaseExclusiveLock();
			map.setRunningTransaction(false);
		}
	}

	public ODocument getConfiguration() {
		return configuration;
	}

	public boolean isAutomatic() {
		return automatic;
	}

	protected void installHooks(final ODatabaseRecord iDatabase) {
		OProfiler.getInstance().registerHookValue("index." + name + ".items", new OProfilerHookValue() {
			public Object getValue() {
				acquireSharedLock();
				try {
					return map != null ? map.size() : "-";
				} finally {
					releaseSharedLock();
				}
			}
		});

		OProfiler.getInstance().registerHookValue("index." + name + ".entryPointSize", new OProfilerHookValue() {
			public Object getValue() {
				return map != null ? map.getEntryPointSize() : "-";
			}
		});

		OProfiler.getInstance().registerHookValue("index." + name + ".maxUpdateBeforeSave", new OProfilerHookValue() {
			public Object getValue() {
				return map != null ? map.getMaxUpdatesBeforeSave() : "-";
			}
		});

		OProfiler.getInstance().registerHookValue("index." + name + ".optimizationThreshold", new OProfilerHookValue() {
			public Object getValue() {
				return map != null ? map.getOptimizeThreshold() : "-";
			}
		});

		Orient.instance().getMemoryWatchDog().addListener(watchDog);
		iDatabase.registerListener(this);
	}

	protected void uninstallHooks(final ODatabaseRecord iDatabase) {
		OProfiler.getInstance().unregisterHookValue("index." + name + ".items");
		OProfiler.getInstance().unregisterHookValue("index." + name + ".entryPointSize");
		OProfiler.getInstance().unregisterHookValue("index." + name + ".maxUpdateBeforeSave");
		OProfiler.getInstance().unregisterHookValue("index." + name + ".optimizationThreshold");
		Orient.instance().getMemoryWatchDog().removeListener(watchDog);
		iDatabase.unregisterListener(this);
	}

	public void onCreate(ODatabase iDatabase) {
	}

	public void onDelete(ODatabase iDatabase) {
	}

	public void onOpen(ODatabase iDatabase) {
	}

	public void onBeforeTxBegin(ODatabase iDatabase) {
	}

	public void onBeforeTxRollback(final ODatabase iDatabase) {
	}

	public void onAfterTxRollback(final ODatabase iDatabase) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			map.unload();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void onBeforeTxCommit(final ODatabase iDatabase) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			map.commitChanges();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void onAfterTxCommit(final ODatabase iDatabase) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			map.onAfterTxCommit();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void onClose(final ODatabase iDatabase) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			map.commitChanges();
			Orient.instance().getMemoryWatchDog().removeListener(watchDog);

		} finally {
			releaseExclusiveLock();
		}
	}

	protected void optimize(final boolean iHardMode) {
		if (map == null)
			return;

		acquireExclusiveLock();
		try {

			OLogManager.instance().debug(this,
					"Forcing " + (iHardMode ? "hard" : "soft") + " optimization of Index %s (%d items). Found %d entries in memory...", name,
					map.size(), map.getInMemoryEntries());

			final int freed = map.optimize(true);

			OLogManager.instance().debug(this, "Completed! Freed %d entries and now %d entries reside in memory", freed,
					map.getInMemoryEntries());

		} finally {
			releaseExclusiveLock();
		}
	}

	protected void checkForKeyType(final Object iKey) {
		if (keyType == null) {
			// RECOGNIZE THE KEY TYPE AT RUN-TIME
			keyType = OType.getTypeByClass(iKey.getClass());

			if (keyType != null)
				updateConfiguration();
		}
	}

	protected void checkForOptimization() {
		if (optimization > 0) {
			final boolean hardMode = optimization == 2;
			optimize(hardMode);
			optimization = 0;
		}
	}

	protected ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	public OType getKeyType() {
		return keyType;
	}

	public OSharedResourceAbstract getLock() {
		return this;
	}
}
