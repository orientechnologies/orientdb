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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

@SuppressWarnings("unchecked")
public class OIndexManagerImpl extends ODocumentWrapperNoClass implements OIndexManager {
	public static final String					CONFIG_INDEXES			= "indexes";
	public static final String					DICTIONARY_NAME			= "dictionary";
	private static final String					QUERY_CREATE				= "create index %s %s";
	private static final String					QUERY_DROP					= "drop index %s";
	private Map<String, OIndexInternal>	indexes							= new HashMap<String, OIndexInternal>();
	private String											defaultClusterName	= OStorage.CLUSTER_INDEX_NAME;

	public OIndexManagerImpl(final ODatabaseRecord iDatabase) {
		super(new ODocument(iDatabase));
	}

	public synchronized OIndexManagerImpl load() {
		if (getDatabase().getStorage().getConfiguration().indexMgrRecordId == null)
			// @COMPATIBILITY: CREATE THE INDEX MGR
			create();

		// CLEAR PREVIOUS STUFF
		indexes.clear();

		// RELOAD IT
		((ORecordId) document.getIdentity()).fromString(getDatabase().getStorage().getConfiguration().indexMgrRecordId);
		super.reload("*:-1 index:0");

		// RE-ASSIGN ALL THE PROPERTY INDEXES, IF ANY
		final Collection<OClass> classes = new ArrayList<OClass>(getDatabase().getMetadata().getSchema().getClasses());
		for (OClass c : classes) {
			for (OProperty p : c.properties()) {
				if (p.getIndex() != null) {
					p.getIndex().delegate = indexes.get(p.getIndex().delegate.getName().toLowerCase());
				}
			}
		}

		return this;
	}

	@Override
	public synchronized <RET extends ODocumentWrapper> RET reload() {
		return (RET) super.reload();
	}

	@Override
	public synchronized <RET extends ODocumentWrapper> RET save() {
		return (RET) super.save();
	}

	public synchronized void create() {
		save(OStorage.CLUSTER_INTERNAL_NAME);
		getDatabase().getStorage().getConfiguration().indexMgrRecordId = document.getIdentity().toString();
		getDatabase().getStorage().getConfiguration().update();

		createIndex(DICTIONARY_NAME, OProperty.INDEX_TYPE.DICTIONARY.toString(), null, null, null, false);
	}

	public synchronized Collection<? extends OIndex> getIndexes() {
		return Collections.unmodifiableCollection(indexes.values());
	}

	public synchronized OIndex getIndex(final String iName) {
		final OIndex index = indexes.get(iName.toLowerCase());
		return new OIndexUser(getDatabase(), getIndexInstance(index));
	}

	public synchronized OIndex getIndexInternal(final String iName) {
		final OIndex index = indexes.get(iName.toLowerCase());
		return getIndexInstance(index);
	}

	public synchronized OIndex getIndex(final ORID iRID) {
		for (OIndex idx : indexes.values()) {
			if (idx.getIdentity().equals(iRID)) {
				return getIndexInstance(idx);
			}
		}
		return null;
	}

	public synchronized OIndex createIndex(final String iName, final String iType, final int[] iClusterIdsToIndex,
			OIndexCallback iCallback, final OProgressListener iProgressListener) {
		return createIndex(iName, iType, iClusterIdsToIndex, iCallback, iProgressListener, false);
	}

	public synchronized OIndex createIndex(final String iName, final String iType, final int[] iClusterIdsToIndex,
			OIndexCallback iCallback, final OProgressListener iProgressListener, final boolean iAutomatic) {
		final String text = String.format(QUERY_CREATE, iName, iType);
		getDatabase().command(new OCommandSQL(text)).execute();

		load();

		return indexes.get(iName.toLowerCase());
	}

	public synchronized OIndex createIndexInternal(final String iName, final String iType, final int[] iClusterIdsToIndex,
			OIndexCallback iCallback, final OProgressListener iProgressListener, final boolean iAutomatic) {

		final OIndexInternal index = OIndexFactory.instance().newInstance(getDatabase(), iType);
		index.setCallback(iCallback);

		reload();

		indexes.put(iName.toLowerCase(), index);

		index.create(iName, getDatabase(), defaultClusterName, iClusterIdsToIndex, iProgressListener, iAutomatic);
		setDirty();
		save();

		return getIndexInstance(index);

	}

	public synchronized OIndexManager dropIndex(final String iIndexName) {
		final String text = String.format(QUERY_DROP, iIndexName);
		getDatabase().command(new OCommandSQL(text)).execute();

		// REMOVE THE INDEX LOCALLY
		indexes.remove(iIndexName.toLowerCase());

		return this;

	}

	public synchronized OIndexManager dropIndexInternal(final String iIndexName) {
		reload();

		final OIndex idx = indexes.remove(iIndexName.toLowerCase());
		if (idx != null) {
			idx.delete();
			setDirty();
			save();
		}
		return this;
	}

	public synchronized OIndexManager setDirty() {
		document.setDirty();
		return this;
	}

	@Override
	protected synchronized void fromStream() {
		final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);

		if (idxs != null) {
			OIndexInternal index;
			for (ODocument d : idxs) {
				index = OIndexFactory.instance().newInstance(getDatabase(), (String) d.field(OIndexInternal.CONFIG_TYPE));
				d.setDatabase(getDatabase());
				((OIndexInternal) index).loadFromConfiguration(d);
				indexes.put(index.getName().toLowerCase(), index);
			}
		}

	}

	/**
	 * Binds POJO to ODocument.
	 */
	@Override
	public ODocument toStream() {
		document.setStatus(ORecordElement.STATUS.UNMARSHALLING);

		try {
			ORecordTrackedSet idxs = new ORecordTrackedSet(document);

			for (OIndexInternal i : indexes.values()) {
				idxs.add(i.updateConfiguration());
			}
			document.field(CONFIG_INDEXES, idxs, OType.EMBEDDEDSET);

		} finally {
			document.setStatus(ORecordElement.STATUS.LOADED);
		}
		document.setDirty();

		return document;
	}

	public synchronized String getDefaultClusterName() {
		return defaultClusterName;
	}

	public synchronized void setDefaultClusterName(String defaultClusterName) {
		this.defaultClusterName = defaultClusterName;
	}

	public synchronized ODictionary<ORecordInternal<?>> getDictionary() {
		return new ODictionary<ORecordInternal<?>>(getIndex(DICTIONARY_NAME));
	}

	private synchronized OIndex getIndexInstance(final OIndex iIndex) {
		if (!(getDatabase().getStorage() instanceof OStorageEmbedded))
			return new OIndexRemote(getDatabase(), iIndex.getName(), iIndex.getType(), iIndex.getIdentity());
		return iIndex;
	}

	private ODatabaseRecord getDatabase() {
		return document.getDatabase();
	}
}
