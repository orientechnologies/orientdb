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
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

@SuppressWarnings("unchecked")
public abstract class OIndexManagerAbstract extends ODocumentWrapperNoClass implements OIndexManager, OCloseable {
	public static final String						CONFIG_INDEXES			= "indexes";
	public static final String						DICTIONARY_NAME			= "dictionary";
	protected Map<String, OIndexInternal>	indexes							= new HashMap<String, OIndexInternal>();
	protected String											defaultClusterName	= OStorage.CLUSTER_INDEX_NAME;

	public OIndexManagerAbstract(final ODatabaseRecord iDatabase) {
		super(new ODocument(iDatabase));
	}

	protected abstract OIndex getIndexInstance(final OIndex iIndex);

	public synchronized OIndexManagerAbstract load() {
		if (getDatabase().getStorage().getConfiguration().indexMgrRecordId == null)
			// @COMPATIBILITY: CREATE THE INDEX MGR
			create();

		// CLEAR PREVIOUS STUFF
		indexes.clear();

		// RELOAD IT
		((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().indexMgrRecordId);
		super.reload("*:-1 index:0");
		return this;
	}

	@Override
	public synchronized <RET extends ODocumentWrapper> RET reload() {
		document.setDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());
		return (RET) super.reload();
	}

	@Override
	public synchronized <RET extends ODocumentWrapper> RET save() {
		document.setDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());
		return (RET) super.save();
	}

	public synchronized void create() {
		save(OStorage.CLUSTER_INTERNAL_NAME);
		getDatabase().getStorage().getConfiguration().indexMgrRecordId = document.getIdentity().toString();
		getDatabase().getStorage().getConfiguration().update();

		createIndex(DICTIONARY_NAME, OProperty.INDEX_TYPE.DICTIONARY.toString(), OType.STRING, null, null, null, false);
	}

	public synchronized void flush() {
		for (OIndexInternal idx : indexes.values())
			idx.flush();
	}

	public synchronized OIndex createIndex(final String iName, final String iType, final OType iKeyType,
			final int[] iClusterIdsToIndex, OIndexCallback iCallback, final OProgressListener iProgressListener) {
		return createIndex(iName, iType, iKeyType, iClusterIdsToIndex, iCallback, iProgressListener, false);
	}

	public synchronized Collection<? extends OIndex> getIndexes() {
		return Collections.unmodifiableCollection(indexes.values());
	}

	public synchronized OIndex getIndex(final String iName) {
		final OIndex index = indexes.get(iName.toLowerCase());
		return new OIndexUser(getDatabase(), getIndexInstance(index));
	}

	public synchronized boolean existsIndex(final String iName) {
		return indexes.containsKey(iName.toLowerCase());
	}

	public synchronized OIndex getIndex(final ORID iRID) {
		for (OIndex idx : indexes.values()) {
			if (idx.getIdentity().equals(iRID)) {
				return getIndexInstance(idx);
			}
		}
		return null;
	}

	public synchronized String getDefaultClusterName() {
		return defaultClusterName;
	}

	public synchronized void setDefaultClusterName(String defaultClusterName) {
		this.defaultClusterName = defaultClusterName;
	}

	public synchronized ODictionary<ORecordInternal<?>> getDictionary() {
		OIndex idx = getIndex(DICTIONARY_NAME);
		if (idx == null)
			idx = createIndex(DICTIONARY_NAME, OProperty.INDEX_TYPE.DICTIONARY.toString(), OType.STRING, null, null, null, false);
		return new ODictionary<ORecordInternal<?>>(idx);
	}

	public ODocument getConfiguration() {
		return getDocument();
	}

	protected ODatabaseRecord getDatabase() {
		document.setDatabase(ODatabaseRecordThreadLocal.INSTANCE.get());
		return document.getDatabase();
	}

	public void close() {
		flush();
		indexes.clear();
	}
}
