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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.util.OMultiKey;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Abstract class to manage indexes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
@SuppressWarnings("unchecked")
public abstract class OIndexManagerAbstract extends ODocumentWrapperNoClass implements OIndexManager, OCloseable {
	public static final String																	CONFIG_INDEXES			= "indexes";
	public static final String																	DICTIONARY_NAME			= "dictionary";
	protected Map<String, OIndexInternal<?>>										indexes							= new HashMap<String, OIndexInternal<?>>();
	protected final Map<String, Map<OMultiKey, Set<OIndex<?>>>>	classPropertyIndex	= new HashMap<String, Map<OMultiKey, Set<OIndex<?>>>>();
	protected String																						defaultClusterName	= OStorage.CLUSTER_INDEX_NAME;

	protected ReadWriteLock																			lock								= new ReentrantReadWriteLock();

	public OIndexManagerAbstract(final ODatabaseRecord iDatabase) {
		super(new ODocument(iDatabase));
	}

	protected abstract OIndex<?> getIndexInstance(final OIndex<?> iIndex);

	protected void acquireSharedLock() {
		lock.readLock().lock();
	}

	protected void releaseSharedLock() {
		lock.readLock().unlock();
	}

	protected void acquireExclusiveLock() {
		lock.writeLock().lock();
	}

	protected void releaseExclusiveLock() {
		lock.writeLock().unlock();
	}

	@Override
	public OIndexManagerAbstract load() {
		acquireExclusiveLock();
		try {

			if (getDatabase().getStorage().getConfiguration().indexMgrRecordId == null)
				// @COMPATIBILITY: CREATE THE INDEX MGR
				create();

			// CLEAR PREVIOUS STUFF
			indexes.clear();
			classPropertyIndex.clear();

			// RELOAD IT
			((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().indexMgrRecordId);
			super.reload("*:-1 index:0");
			return this;
		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public <RET extends ODocumentWrapper> RET reload() {
		acquireExclusiveLock();
		try {
			return (RET) super.reload();
		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public <RET extends ODocumentWrapper> RET save() {
		acquireExclusiveLock();
		try {
			return (RET) super.save();
		} finally {
			releaseExclusiveLock();
		}
	}

	public void create() {
		acquireExclusiveLock();
		try {
			save(OStorage.CLUSTER_INTERNAL_NAME);
			getDatabase().getStorage().getConfiguration().indexMgrRecordId = document.getIdentity().toString();
			getDatabase().getStorage().getConfiguration().update();

			createIndex(DICTIONARY_NAME, OClass.INDEX_TYPE.DICTIONARY.toString(), new OSimpleKeyIndexDefinition(OType.STRING), null, null);
		} finally {
			releaseExclusiveLock();
		}
	}

	public void flush() {
		acquireExclusiveLock();
		try {

			for (final OIndexInternal<?> idx : indexes.values())
				idx.flush();
		} finally {
			releaseExclusiveLock();
		}
	}

	public Collection<? extends OIndex<?>> getIndexes() {
		acquireSharedLock();
		try {
			final Collection<OIndexInternal<?>> rawResult = indexes.values();
			final List<OIndex<?>> result = new ArrayList<OIndex<?>>(rawResult.size());
			for (final OIndex<?> index : rawResult)
				result.add(wrapInTransactional(index));
			return result;
		} finally {
			releaseSharedLock();
		}

	}

	public OIndex<?> getIndex(final String iName) {
		acquireSharedLock();

		try {
			final OIndex<?> index = indexes.get(iName.toLowerCase());
			if (index == null)
				return null;

			return wrapInTransactional(index);
		} finally {
			releaseSharedLock();
		}

	}

	public boolean existsIndex(final String iName) {
		acquireSharedLock();
		try {
			return indexes.containsKey(iName.toLowerCase());
		} finally {
			releaseSharedLock();
		}
	}

	public String getDefaultClusterName() {
		acquireSharedLock();
		try {
			return defaultClusterName;
		} finally {
			releaseSharedLock();
		}
	}

	public void setDefaultClusterName(final String defaultClusterName) {
		acquireExclusiveLock();
		try {
			this.defaultClusterName = defaultClusterName;
		} finally {
			releaseExclusiveLock();
		}
	}

	public ODictionary<ORecordInternal<?>> getDictionary() {
		acquireExclusiveLock();

		OIndex<?> idx;
		try {
			idx = getIndex(DICTIONARY_NAME);
			if (idx == null)
				idx = createIndex(DICTIONARY_NAME, OClass.INDEX_TYPE.DICTIONARY.toString(), new OSimpleKeyIndexDefinition(OType.STRING),
						null, null);
		} finally {
			releaseExclusiveLock();
		}

		return new ODictionary<ORecordInternal<?>>((OIndex<OIdentifiable>) idx);
	}

	public ODocument getConfiguration() {
		acquireSharedLock();

		try {
			return getDocument();
		} finally {
			releaseSharedLock();
		}

	}

	protected ODatabaseRecord getDatabase() {
		return ODatabaseRecordThreadLocal.INSTANCE.get();
	}

	public void close() {
		acquireExclusiveLock();
		try {
			flush();
			indexes.clear();
			classPropertyIndex.clear();
		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexManager setDirty() {
		acquireExclusiveLock();
		try {
			document.setDirty();
			return this;
		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndex<?> getIndex(final ORID iRID) {
		acquireSharedLock();
		try {
			for (final OIndex<?> idx : indexes.values()) {
				if (idx.getIdentity().equals(iRID)) {
					return getIndexInstance(idx);
				}
			}
			return null;
		} finally {
			releaseSharedLock();
		}
	}

	protected void addIndexInternal(final OIndexInternal<?> index) {
		acquireExclusiveLock();
		try {
			indexes.put(index.getName().toLowerCase(), index);

			final OIndexDefinition indexDefinition = index.getDefinition();
			if (indexDefinition == null || indexDefinition.getClassName() == null)
				return;

			Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(indexDefinition.getClassName().toLowerCase());

			if (propertyIndex == null) {
				propertyIndex = new HashMap<OMultiKey, Set<OIndex<?>>>();
				classPropertyIndex.put(indexDefinition.getClassName().toLowerCase(), propertyIndex);
			}

			final int paramCount = indexDefinition.getParamCount();

			for (int i = 1; i <= paramCount; i++) {
				final List<String> fields = indexDefinition.getFields().subList(0, i);
				final OMultiKey multiKey = new OMultiKey(normalizeFieldNames(fields));
				Set<OIndex<?>> indexSet = propertyIndex.get(multiKey);
				if (indexSet == null)
					indexSet = new HashSet<OIndex<?>>();
				indexSet.add(index);
				propertyIndex.put(multiKey, indexSet);
			}
		} finally {
			releaseExclusiveLock();
		}
	}

	public Set<OIndex<?>> getClassInvolvedIndexes(final String className, Collection<String> fields) {
		acquireSharedLock();
		try {
			fields = normalizeFieldNames(fields);

			final OMultiKey multiKey = new OMultiKey(fields);

			final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(className.toLowerCase());

			if (propertyIndex == null || !propertyIndex.containsKey(multiKey))
				return Collections.emptySet();

			final Set<OIndex<?>> rawResult = propertyIndex.get(multiKey);
			final Set<OIndex<?>> transactionalResult = new HashSet<OIndex<?>>(rawResult.size());
			for (final OIndex<?> index : rawResult) {
				transactionalResult.add(wrapInTransactional(index));
			}

			return transactionalResult;
		} finally {
			releaseSharedLock();
		}
	}

	public Set<OIndex<?>> getClassInvolvedIndexes(final String className, final String... fields) {
		return getClassInvolvedIndexes(className, Arrays.asList(fields));
	}

	public boolean areIndexed(final String className, Collection<String> fields) {
		acquireSharedLock();
		try {
			fields = normalizeFieldNames(fields);

			final OMultiKey multiKey = new OMultiKey(fields);

			final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(className.toLowerCase());

			if (propertyIndex == null)
				return false;

			return propertyIndex.containsKey(multiKey) && !propertyIndex.get(multiKey).isEmpty();
		} finally {
			releaseSharedLock();
		}
	}

	public boolean areIndexed(final String className, final String... fields) {
		return areIndexed(className, Arrays.asList(fields));
	}

	public Set<OIndex<?>> getClassIndexes(final String className) {
		acquireSharedLock();
		try {
			final Set<OIndex<?>> result = new HashSet<OIndex<?>>();
			final Map<OMultiKey, Set<OIndex<?>>> propertyIndex = classPropertyIndex.get(className.toLowerCase());

			if (propertyIndex == null)
				return Collections.emptySet();

			for (final Set<OIndex<?>> propertyIndexes : propertyIndex.values())
				for (final OIndex<?> index : propertyIndexes)
					result.add(wrapInTransactional(index));
			return result;
		} finally {
			releaseSharedLock();
		}
	}

	public OIndex<?> getClassIndex(String className, String indexName) {
		acquireSharedLock();
		try {
			className = className.toLowerCase();
			indexName = indexName.toLowerCase();
			final OIndex<?> index = indexes.get(indexName);
			if (index != null && index.getDefinition() != null && index.getDefinition().getClassName() != null
					&& className.equals(index.getDefinition().getClassName().toLowerCase()))
				return wrapInTransactional(index);
			return null;
		} finally {
			releaseSharedLock();
		}
	}

	protected List<String> normalizeFieldNames(final Collection<String> fieldNames) {
		final ArrayList<String> result = new ArrayList<String>(fieldNames.size());
		for (final String fieldName : fieldNames)
			result.add(fieldName.toLowerCase());
		return result;
	}

	private OIndex<?> wrapInTransactional(final OIndex<?> index) {
		if (index instanceof OIndexMultiValues)
			return new OIndexTxAwareMultiValue(getDatabase(), (OIndex<Collection<OIdentifiable>>) getIndexInstance(index));
		else if (index instanceof OIndexOneValue)
			return new OIndexTxAwareOneValue(getDatabase(), (OIndex<OIdentifiable>) getIndexInstance(index));
		return index;
	}
}
