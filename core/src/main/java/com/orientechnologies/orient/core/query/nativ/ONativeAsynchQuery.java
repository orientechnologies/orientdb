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

import com.orientechnologies.orient.core.exception.OQueryExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.OAsynchQueryResultListener;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

public abstract class ONativeAsynchQuery<T extends ORecordInternal<?>, CTX extends OQueryContextNative<T>> extends
		ONativeQuery<T, CTX> implements ORecordBrowsingListener {
	protected OAsynchQueryResultListener<T>	resultListener;
	protected int														resultCount	= 0;

	public ONativeAsynchQuery(String iCluster, CTX iQueryRecordImpl) {
		this(iCluster, iQueryRecordImpl, null);
	}

	public ONativeAsynchQuery(String iCluster, CTX iQueryRecordImpl, OAsynchQueryResultListener<T> iResultListener) {
		super(iCluster);
		resultListener = iResultListener;
		queryRecord = iQueryRecordImpl;
	}

	public boolean isAsynchronous() {
		return resultListener != this;
	}

	@SuppressWarnings("unchecked")
	public boolean foreach(ORecordInternal<?> iRecord) {
		T record = (T) iRecord;
		queryRecord.setRecord(record);

		if (filter(queryRecord)) {
			resultCount++;
			resultListener.result((T) record.copy());

			if (limit > -1 && resultCount == limit)
				// BREAK THE EXECUTION
				return false;
		}
		return true;
	}

	public List<T> execute(int iLimit) {
		limit = iLimit;
		queryRecord.setSourceQuery(this);

		// CHECK IF A CLASS WAS CREATED
		OClass cls = database.getMetadata().getSchema().getClass(cluster);
		if (cls == null)
			throw new OQueryExecutionException("Cluster " + cluster + " was not found");

		((OStorageLocal) database.getStorage()).browse(database.getId(), cls.getClusterIds(), this, record);
		return null;
	}

	public T executeFirst() {
		execute(1);
		return null;
	}

	public OAsynchQueryResultListener<T> getResultListener() {
		return resultListener;
	}

	public void setResultListener(OAsynchQueryResultListener<T> resultListener) {
		this.resultListener = resultListener;
	}
}
