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
package com.orientechnologies.orient.core.query.nativ;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

@SuppressWarnings("serial")
public abstract class ONativeAsynchQuery<CTX extends OQueryContextNative> extends ONativeQuery<CTX> implements
		ORecordBrowsingListener {
	protected OCommandResultListener	resultListener;
	protected int							resultCount	= 0;
	protected ORecordInternal<?>		record;

	public ONativeAsynchQuery(final ODatabaseRecord iDatabase, final String iCluster, final CTX iQueryRecordImpl) {
		this(iDatabase, iCluster, iQueryRecordImpl, null);
	}

	public ONativeAsynchQuery(final ODatabaseRecord iDatabase, final String iCluster, final CTX iQueryRecordImpl,
			final OCommandResultListener iResultListener) {
		super(iDatabase, iCluster);
		resultListener = iResultListener;
		queryRecord = iQueryRecordImpl;
		record = new ODocument(database);
	}

	public boolean isAsynchronous() {
		return resultListener != this;
	}

	public boolean foreach(final ORecordInternal<?> iRecord) {
		final ODocument record = (ODocument) iRecord;
		queryRecord.setRecord(record);

		if (filter(queryRecord)) {
			resultCount++;
			resultListener.result(record.copy());

			if (limit > -1 && resultCount == limit)
				// BREAK THE EXECUTION
				return false;
		}
		return true;
	}

	public List<ODocument> run(final Object... iArgs) {
		if (!(database.getStorage() instanceof OStorageEmbedded))
			throw new OCommandExecutionException("Native queries can run only in embedded-local version. Not in the remote one.");

		queryRecord.setSourceQuery(this);

		// CHECK IF A CLASS WAS CREATED
		final OClass cls = database.getMetadata().getSchema().getClass(cluster);
		if (cls == null)
			throw new OCommandExecutionException("Cluster " + cluster + " was not found");

		((OStorageEmbedded) database.getStorage()).browse(cls.getPolymorphicClusterIds(), null, null, this, record, false);
		return null;
	}

	public ODocument runFirst(final Object... iArgs) {
		setLimit(1);
		execute();
		return null;
	}

	@Override
	public OCommandResultListener getResultListener() {
		return resultListener;
	}

	@Override
	public void setResultListener(final OCommandResultListener resultListener) {
		this.resultListener = resultListener;
	}
}
