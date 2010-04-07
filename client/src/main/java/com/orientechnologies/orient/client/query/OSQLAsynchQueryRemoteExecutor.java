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
package com.orientechnologies.orient.client.query;

import java.util.List;

import com.orientechnologies.orient.client.storage.OStorageRemote;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.query.OQueryExecutor;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * Deleagate the execution to the server side engine.
 * 
 * @author luca
 * 
 */
@SuppressWarnings("unchecked")
public class OSQLAsynchQueryRemoteExecutor implements OQueryExecutor {
	public static final OQueryExecutor	INSTANCE	= new OSQLAsynchQueryRemoteExecutor();

	public <R extends ORecordSchemaAware<?>> List<R> execute(final OQuery<R> iQuery, final int iLimit) {
		return (List<R>) ((OStorageRemote) iQuery.getDatabase().getStorage()).query(iQuery, iLimit);
	}

	public <R extends ORecordSchemaAware<?>> R executeFirst(final OQuery<R> iQuery) {
		return (R) ((OStorageRemote) iQuery.getDatabase().getStorage()).queryFirst(iQuery);
	}
}
