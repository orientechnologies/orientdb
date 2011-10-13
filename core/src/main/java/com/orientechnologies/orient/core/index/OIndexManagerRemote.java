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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordTrackedSet;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

public class OIndexManagerRemote extends OIndexManagerAbstract {
	private static final String	QUERY_CREATE	= "create index %s %s %s";
	private static final String	QUERY_DROP		= "drop index %s";

	public OIndexManagerRemote(final ODatabaseRecord iDatabase) {
		super(iDatabase);
	}

	protected OIndex<?> getIndexInstance(final OIndex<?> iIndex) {
		if (iIndex instanceof OIndexMultiValues)
			return new OIndexRemoteMultiValue(iIndex.getName(), iIndex.getType(), iIndex.getIdentity(), iIndex.getDefinition(),
					getConfiguration());
		return new OIndexRemoteOneValue(iIndex.getName(), iIndex.getType(), iIndex.getIdentity(), iIndex.getDefinition(),
				getConfiguration());
	}

	public OIndex createIndex(final String iName, final String iType, final OIndexDefinition iIndexDefinition,
			final int[] iClusterIdsToIndex, final OProgressListener iProgressListener) {
		final String createIndexDDL;
		if (iIndexDefinition != null) {
			createIndexDDL = iIndexDefinition.toCreateIndexDDL(iName, iType);
		} else {
			createIndexDDL = new OSimpleKeyIndexDefinition().toCreateIndexDDL(iName, iType);
		}

		acquireExclusiveLock();
		try {
			if (iProgressListener != null) {
				iProgressListener.onBegin(this, 0);
			}

			getDatabase().command(new OCommandSQL(createIndexDDL)).execute();
			document.setDatabase(getDatabase());
			document.setIdentity(new ORecordId(document.getDatabase().getStorage().getConfiguration().indexMgrRecordId));

			if (iProgressListener != null) {
				iProgressListener.onCompletition(this, true);
			}

			reload();

			return indexes.get(iName.toLowerCase());
		} finally {
			releaseExclusiveLock();
		}
	}

	public OIndexManager dropIndex(final String iIndexName) {
		acquireExclusiveLock();
		try {
			final String text = String.format(QUERY_DROP, iIndexName);
			getDatabase().command(new OCommandSQL(text)).execute();

			// REMOVE THE INDEX LOCALLY
			indexes.remove(iIndexName.toLowerCase());
			reload();

			return this;
		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	protected void fromStream() {
		acquireExclusiveLock();

		try {
			document.setDatabase(getDatabase());
			final Collection<ODocument> idxs = document.field(CONFIG_INDEXES);

			indexes.clear();
			classPropertyIndex.clear();

			if (idxs != null) {
				OIndexInternal<?> index;
				for (ODocument d : idxs) {
					index = OIndexFactory.instance().newInstance(getDatabase(), (String) d.field(OIndexInternal.CONFIG_TYPE));
					d.setDatabase(getDatabase());
					((OIndexInternal<?>) index).loadFromConfiguration(d);
					addIndexInternal(index);
				}
			}
		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Binds POJO to ODocument.
	 */
	@Override
	public ODocument toStream() {
		acquireExclusiveLock();

		try {
			document.setDatabase(getDatabase());
			document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

			try {
				ORecordTrackedSet idxs = new ORecordTrackedSet(document);

				for (OIndexInternal<?> i : indexes.values()) {
					idxs.add(i.updateConfiguration());
				}
				document.field(CONFIG_INDEXES, idxs, OType.EMBEDDEDSET);

			} finally {
				document.setInternalStatus(ORecordElement.STATUS.LOADED);
			}
			document.setDirty();

			return document;
		} finally {
			releaseExclusiveLock();
		}
	}
}
