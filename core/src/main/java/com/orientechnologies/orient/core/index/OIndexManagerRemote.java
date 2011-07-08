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
import com.orientechnologies.orient.core.id.ORID;
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

	protected synchronized OIndex getIndexInstance(final OIndex iIndex) {
		return new OIndexRemote(iIndex.getName(), iIndex.getType(), iIndex.getIdentity());
	}

	public synchronized OIndex getIndex(final String iName) {
		final OIndex index = indexes.get(iName.toLowerCase());
		return index != null ? new OIndexUser(getDatabase(), getIndexInstance(index)) : null;
	}

	public synchronized OIndex getIndex(final ORID iRID) {
		for (OIndex idx : indexes.values()) {
			if (idx.getIdentity().equals(iRID)) {
				return getIndexInstance(idx);
			}
		}
		return null;
	}

	public synchronized OIndex createIndex(final String iName, final String iType, final OType iKeyType,
			final int[] iClusterIdsToIndex, OIndexCallback iCallback, final OProgressListener iProgressListener, final boolean iAutomatic) {
		final String text = String.format(QUERY_CREATE, iName, iType, iKeyType);
		getDatabase().command(new OCommandSQL(text)).execute();

		document.setIdentity(new ORecordId(document.getDatabase().getStorage().getConfiguration().indexMgrRecordId));
		reload();

		return indexes.get(iName.toLowerCase());
	}

	public synchronized OIndexManager dropIndex(final String iIndexName) {
		final String text = String.format(QUERY_DROP, iIndexName);
		getDatabase().command(new OCommandSQL(text)).execute();

		// REMOVE THE INDEX LOCALLY
		indexes.remove(iIndexName.toLowerCase());

		return this;
	}

	public OIndexManager dropClassIndexes(String iIndexName) {
		return null;
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
}
