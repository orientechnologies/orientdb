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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.query.OAsynchQueryResultListener;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

public class OSQLSynchQuery<T extends ORecordSchemaAware<?>> extends OSQLAsynchQuery<T> implements OAsynchQueryResultListener<T> {
	protected final List<T>	result	= new ArrayList<T>();

	public OSQLSynchQuery() {
		resultListener = this;
	}

	public OSQLSynchQuery(final String iText) {
		super(iText);
		resultListener = this;
	}

	public boolean result(final T iRecord) {
		result.add(iRecord);
		return true;
	}

	@Override
	public List<T> execute(final int iLimit) {
		super.execute(iLimit);
		return result;
	}
}
