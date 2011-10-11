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

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.resource.OSharedResourceAbstract;
import com.orientechnologies.common.concur.resource.OSharedResourceExternal;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.common.profiler.OProfiler.OProfilerHookValue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.annotation.ODocumentInstance;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog.Listener;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.serialization.serializer.stream.OCompositeKeySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerLiteral;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabaseLazySave;

/**
 * Handles indexing when records change.
 *
 * @author Luca Garulli
 *
 */
public abstract class OIndexMVRBTreeAbstract<T> extends OSharedResourceExternal implements OIndexInternal<T>, ODatabaseListener {
	protected static final String										CONFIG_MAP_RID	= "mapRid";
	protected static final String										CONFIG_CLUSTERS	= "clusters";
	protected String																name;
	protected String																type;
	protected OMVRBTreeDatabaseLazySave<Object, T>	map;
	protected Set<String> clustersToIndex	= new LinkedHashSet<String>();
	protected OIndexDefinition																indexDefinition;

	@ODocumentInstance
	protected ODocument															configuration;
	private Listener																watchDog;

	public OIndexMVRBTreeAbstract(final String iType) {
		type = iType;

		watchDog = new Listener() {
			public void memoryUsageLow(final TYPE iType, final long usedMemory, final long maxMemory) {
				if (iType == TYPE.JVM)
					map.setOptimization(1);
			}

			public void memoryUsageCritical(final TYPE iType, final long usedMemory, final long maxMemory) {
				if (iType == TYPE.JVM)
					map.setOptimization(2);
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
     * @param iClusterIndexName
*          Cluster name where to place the TreeMap
     * @param iProgressListener
     */
	public OIndexInternal create(final String iName, final OIndexDefinition iIndexDefinition, final ODatabaseRecord iDatabase,
                                 final String iClusterIndexName, final int[] iClusterIdsToIndex, final OProgressListener iProgressListener,
                                 final OStreamSerializer iValueSerializer) {
		acquireExclusiveLock();
		try {

			name = iName;
			configuration = new ODocument(iDatabase);

			indexDefinition = iIndexDefinition;

			if (iClusterIdsToIndex != null)
				for (int id : iClusterIdsToIndex)
					clustersToIndex.add(iDatabase.getClusterNameById(id));

            final OStreamSerializer keySerializer;
            if(indexDefinition instanceof OCompositeIndexDefinition)
                keySerializer = OCompositeKeySerializer.INSTANCE;
            else
                keySerializer = OStreamSerializerLiteral.INSTANCE;

			map = new OMVRBTreeDatabaseLazySave<Object, T>(iDatabase, iClusterIndexName, keySerializer,
					iValueSerializer);

			installHooks(iDatabase);

			rebuild(iProgressListener);
			updateConfiguration();
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexInternal<T> loadFromConfiguration(final ODocument iConfig) {
		acquireExclusiveLock();
		try {

			final ORID rid = (ORID) iConfig.field(CONFIG_MAP_RID, ORID.class);
			if (rid == null)
				return null;

            configuration = iConfig;
            name = configuration.field(OIndexInternal.CONFIG_NAME);

            final ODocument indexDefinitionDoc = configuration.field(OIndexInternal.INDEX_DEFINITION);
            if (indexDefinitionDoc != null) {
                try {
                    final String indexDefClassName = configuration.field(OIndexInternal.INDEX_DEFINITION_CLASS);
                    final Class  indexDefClass = Class.forName(indexDefClassName);
                    indexDefinition = (OIndexDefinition)indexDefClass.getDeclaredConstructor().newInstance();
                    indexDefinition.fromStream(indexDefinitionDoc);

                } catch (ClassNotFoundException e) {
                    throw new OIndexException("Error during deserialization of index definition", e);
                } catch (NoSuchMethodException e) {
                    throw new OIndexException("Error during deserialization of index definition", e);
                } catch (InvocationTargetException e) {
                    throw new OIndexException("Error during deserialization of index definition", e);
                } catch (InstantiationException e) {
                    throw new OIndexException("Error during deserialization of index definition", e);
                } catch (IllegalAccessException e) {
                    throw new OIndexException("Error during deserialization of index definition", e);
                }
            } else {
               //@COMPATIBILITY 1.0rc6 new index model was implemented
               final Boolean isAutomatic = configuration.field(OIndexInternal.CONFIG_AUTOMATIC);
               if(Boolean.TRUE.equals(isAutomatic)) {
                   final int pos = name.lastIndexOf('.');
                   if(pos < 0)
                       throw new OIndexException("Can not convert from old index model to new one. " +
                               "Invalid index name. Dot (.) separator should be present.");
                   final String className = name.substring(0, pos);
                   final String propertyName = name.substring(pos + 1);

                   final String keyTypeStr = configuration.field(OIndexInternal.CONFIG_KEYTYPE);
                   if(keyTypeStr == null)
                       throw new OIndexException("Can not convert from old index model to new one. " +
                               "Index key type is absent.");
                   final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));
                   indexDefinition = new OPropertyIndexDefinition(className, propertyName,keyType);

                   configuration.removeField(OIndexInternal.CONFIG_AUTOMATIC);
                   configuration.removeField(OIndexInternal.CONFIG_KEYTYPE);
               } else if(configuration.field(OIndexInternal.CONFIG_KEYTYPE) != null) {
                   final String keyTypeStr = configuration.field(OIndexInternal.CONFIG_KEYTYPE);
                   final OType keyType = OType.valueOf(keyTypeStr.toUpperCase(Locale.ENGLISH));

                   indexDefinition = new OSimpleKeyIndexDefinition(keyType);

                   configuration.removeField(OIndexInternal.CONFIG_KEYTYPE);
               }
            }

			clustersToIndex.clear();

			final Collection<? extends String> clusters = configuration.field(CONFIG_CLUSTERS);
			if (clusters != null)
				clustersToIndex.addAll(clusters);

			map = new OMVRBTreeDatabaseLazySave<Object, T>(getDatabase(), rid);
			map.load();

			installHooks(iConfig.getDatabase());

			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public boolean contains(final Object iKey) {

		acquireExclusiveLock();
		try {

			return map.containsKey(iKey);

		} finally {
			releaseExclusiveLock();
		}
	}

    /**
     * Returns a set of records with key between the range passed as parameter. Range bounds are included.
     * <p/>
     * In case of {@link com.orientechnologies.common.collection.OCompositeKey}s partial keys can be used
     * as values boundaries.
     *
     * @param iRangeFrom Starting range
     * @param iRangeTo   Ending range
     * @return a set of records with key between the range passed as parameter. Range bounds are included.
     * @see com.orientechnologies.common.collection.OCompositeKey#compareTo(com.orientechnologies.common.collection.OCompositeKey)
     * @see #getValuesBetween(Object, boolean, Object, boolean)
     */
    public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo) {
        return getValuesBetween(iRangeFrom, true, iRangeTo, true);
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
	public Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo) {
		return getEntriesBetween(iRangeFrom, iRangeTo, true);
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
						final Object fieldValue = indexDefinition.getDocumentValueToIndex(doc);

						if (fieldValue != null) {
                            if(fieldValue instanceof Collection) {
                                for(final Object fieldValueItem : (Collection)fieldValue) {
                                    put(fieldValueItem, doc);
                                }
                            } else
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

		acquireExclusiveLock();
		try {

			return map.remove(key) != null;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndex<T> clear() {

		acquireExclusiveLock();
		try {

			map.clear();
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexInternal<T> delete() {

		acquireExclusiveLock();

		try {
			map.delete();
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexInternal<T> lazySave() {

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

	public Iterator<Entry<Object, T>> iterator() {

		acquireExclusiveLock();
		try {

			return map.entrySet().iterator();

		} finally {
			releaseExclusiveLock();
		}
	}

	public Iterable<Object> keys() {

		acquireExclusiveLock();
		try {

			return map.keySet();

		} finally {
			releaseExclusiveLock();
		}
	}

	public long getSize() {

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

	public OIndexInternal<T> getInternal() {
		return this;
	}


	public Set<String> getClusters() {

		acquireSharedLock();
		try {

			return Collections.unmodifiableSet(clustersToIndex);

		} finally {
			releaseSharedLock();
		}
	}

	public OIndexMVRBTreeAbstract<T> addCluster(final String iClusterName) {

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

		acquireExclusiveLock();
		try {

			map.unload();

		} finally {
			releaseExclusiveLock();
		}
	}

	public ODocument updateConfiguration() {

		acquireExclusiveLock();
		try {

			configuration.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

			try {
				configuration.field(OIndexInternal.CONFIG_TYPE, type);
				configuration.field(OIndexInternal.CONFIG_NAME, name);

                if(indexDefinition != null) {
                    final ODocument indexDefDocument = indexDefinition.toStream();
                    if(!indexDefDocument.hasOwners())
                        indexDefDocument.addOwner(configuration);

                    configuration.field(OIndexInternal.INDEX_DEFINITION, indexDefDocument, OType.EMBEDDED);
                    configuration.field(OIndexInternal.INDEX_DEFINITION_CLASS, indexDefinition.getClass().getName());
                }
                else {
                    configuration.removeField(OIndexInternal.INDEX_DEFINITION);
                    configuration.removeField(OIndexInternal.INDEX_DEFINITION_CLASS);
                }

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
		return indexDefinition != null && indexDefinition.getClassName() != null;
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

		acquireExclusiveLock();
		try {

			map.unload();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void onBeforeTxCommit(final ODatabase iDatabase) {
		acquireExclusiveLock();
		try {

			map.commitChanges();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void onAfterTxCommit(final ODatabase iDatabase) {
		acquireExclusiveLock();
		try {

			map.onAfterTxCommit();

		} finally {
			releaseExclusiveLock();
		}
	}

	public void onClose(final ODatabase iDatabase) {
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
					map.size(), map.getNumberOfNodesInCache());

			map.setOptimization(iHardMode ? 2 : 1);
			final int freed = map.optimize(iHardMode);

			OLogManager.instance().debug(this, "Completed! Freed %d entries and now %d entries reside in memory", freed,
                    map.getNumberOfNodesInCache());

		} finally {
			releaseExclusiveLock();
		}
	}

	protected void checkForKeyType(final Object iKey) {
		if (indexDefinition == null) {
			// RECOGNIZE THE KEY TYPE AT RUN-TIME

            final OType type = OType.getTypeByClass(iKey.getClass());
            if(type == null)
                return;

			indexDefinition = new OSimpleKeyIndexDefinition(type);

		updateConfiguration();
		}
	}

	protected ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	public OType[] getKeyTypes() {
        if(indexDefinition == null)
            return null;

		return indexDefinition.getTypes();
	}

    public OIndexDefinition getDefinition() {
        return indexDefinition;
    }

    public OSharedResourceAbstract getLock() {
		return this;
	}

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final OIndexMVRBTreeAbstract that = (OIndexMVRBTreeAbstract) o;

        if (!name.equals(that.name))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
