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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

public class OIndexManager extends ODocumentWrapperNoClass {
	public static final String	CONFIG_INDEXES			= "indexes";
	private ODatabaseRecord			database;
	private Map<String, OIndex>	indexes							= new HashMap<String, OIndex>();
	private String							defaultClusterName	= OStorage.CLUSTER_INDEX_NAME;

	public OIndexManager(final ODatabaseRecord iDatabase) {
		super(new ODocument(iDatabase));
		this.database = iDatabase;
	}

	@SuppressWarnings("unchecked")
	public OIndexManager load() {
		if (document.getDatabase().getStorage().getConfiguration().indexMgrRecordId == null)
			// @COMPATIBILITY: CREATE THE INDEX MGR
			create();

		((ORecordId) document.getIdentity()).fromString(document.getDatabase().getStorage().getConfiguration().indexMgrRecordId);
		super.load("*:-1 index:0");
		return this;
	}

	public void create() {
		save(OStorage.CLUSTER_INTERNAL_NAME);
		document.getDatabase().getStorage().getConfiguration().indexMgrRecordId = document.getIdentity().toString();
		document.getDatabase().getStorage().getConfiguration().update(database.getId());
	}

	public Collection<OIndex> getIndexes() {
		return Collections.unmodifiableCollection(indexes.values());
	}

	public OIndex getIndex(final String iName) {
		return indexes.get(iName.toLowerCase());
	}

	public OIndex getIndex(final ORecordId iRID) {
		for (OIndex idx : indexes.values()) {
			if (idx.getIdentity().equals(iRID))
				return idx;
		}
		return null;
	}

	public OIndex createIndex(final OIndex iIndex) {
		indexes.put(iIndex.getName().toLowerCase(), iIndex);
		setDirty();
		save();
		return iIndex;
	}

	public OIndex createIndex(final String iName, final String iType, final int[] iClusterIdsToIndex, OIndexCallback iCallback,
			final OProgressListener iProgressListener) {
		return createIndex(iName, iType, iClusterIdsToIndex, iCallback, iProgressListener, false);
	}

	public OIndex createIndex(final String iName, final String iType, final int[] iClusterIdsToIndex, OIndexCallback iCallback,
			final OProgressListener iProgressListener, final boolean iAutomatic) {
		final OIndex index = OIndexFactory.instance().newInstance(iType);
		index.setCallback(iCallback);
		indexes.put(iName.toLowerCase(), index);

		index.create(iName, database, defaultClusterName, iClusterIdsToIndex, iProgressListener, iAutomatic);
		setDirty();
		save();

		return index;
	}

	public OIndexManager deleteIndex(final String iIndexName) {
		final OIndex idx = indexes.remove(iIndexName.toLowerCase());
		if (idx != null) {
			idx.delete();
			setDirty();
			save();
		}
		return this;
	}

	public OIndexManager setDirty() {
		document.setDirty();
		return this;
	}

	@Override
	protected void fromStream() {
		final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);

		if (idxs != null) {
			OIndex index;
			for (ODocument d : idxs) {
				index = OIndexFactory.instance().newInstance((String) d.field(OIndex.CONFIG_TYPE));
				d.setDatabase(document.getDatabase());
				index.loadFromConfiguration(d);
				indexes.put(index.getName().toLowerCase(), index);
			}
		}
	}

	/**
	 * Binds POJO to ODocument.
	 */
	@Override
	public ODocument toStream() {
		document.setStatus(STATUS.UNMARSHALLING);

		try {
			ORecordTrackedSet idxs = new ORecordTrackedSet(document);

			for (OIndex i : indexes.values()) {
				idxs.add(i.updateConfiguration());
			}
			document.field(CONFIG_INDEXES, idxs, OType.EMBEDDEDSET);

		} finally {
			document.setStatus(STATUS.LOADED);
		}
		document.setDirty();

		return document;
	}

	/**
	 * Load a previously created index. This method is kept for compatibility with 0.9.24 databases
	 * 
	 * @COMPATIBILITY
	 */
	public OIndex loadIndex(final String iName, final ODocument iConfiguration) {
		final OIndex index = OIndexFactory.instance().load(database, iConfiguration);
		if (index != null) {
			indexes.put(iName.toLowerCase(), index);
			setDirty();
			save();
		}

		return index;
	}

	public String getDefaultClusterName() {
		return defaultClusterName;
	}

	public void setDefaultClusterName(String defaultClusterName) {
		this.defaultClusterName = defaultClusterName;
	}
}
