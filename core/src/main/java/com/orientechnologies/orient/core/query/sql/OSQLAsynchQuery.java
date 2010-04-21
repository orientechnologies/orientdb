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
package com.orientechnologies.orient.core.query.sql;

import java.util.List;

import com.orientechnologies.orient.core.query.OAsynchQuery;
import com.orientechnologies.orient.core.query.OAsynchQueryResultListener;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.storage.ORecordBrowsingListener;

/**
 * SQL asynchronous query. When executed the caller doesn't wait the the execution, rather the listener will be called foreach item
 * found in the query. OSQLAsynchQuery has been built on top of this.
 * 
 * @author luca
 * 
 * @param <T>
 * @see OSQLSynchQuery
 */
public class OSQLAsynchQuery<T extends ORecordSchemaAware<?>> extends OSQLQuery<T> implements OAsynchQuery<T>,
		ORecordBrowsingListener {
	protected OAsynchQueryResultListener<T>	resultListener;
	protected int														resultCount	= 0;
	protected OSQLQueryCompiled							compiled;

	/**
	 * Empty constructor for unmarshalling.
	 */
	public OSQLAsynchQuery() {
	}

	public OSQLAsynchQuery(final String iText) {
		this(iText, null);
	}

	public OSQLAsynchQuery(final String iText, final OAsynchQueryResultListener<T> iResultListener) {
		super(iText);
		resultListener = iResultListener;
	}

	public Object execute(final String iText) {
		return execute(iText, -1);
	}

	public Object execute(final String iText, final int iLimit) {
		compiled = null;
		return execute(iLimit);
	}

	public List<T> execute() {
		return execute(-1);
	}

	public T executeFirst() {
		execute(1);
		return null;
	}

	@SuppressWarnings("unchecked")
	public boolean foreach(final ORecordInternal<?> iRecord) {
		T record = (T) iRecord;

		if (filter(record)) {
			resultCount++;
			resultListener.result((T) record.copy());

			if (limit > -1 && resultCount == limit)
				// BREAK THE EXECUTION
				return false;
		}
		return true;
	}

	protected boolean filter(final T iRecord) {
		return compiled.evaluate(database, iRecord);
	}

	public OAsynchQueryResultListener<T> getResultListener() {
		return resultListener;
	}

	public void setResultListener(final OAsynchQueryResultListener<T> resultListener) {
		this.resultListener = resultListener;
	}

	protected void parse() {
		if (compiled == null)
			compiled = new OSQLQueryCompiled(this, text);
	}
}
