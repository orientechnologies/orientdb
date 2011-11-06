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
package com.orientechnologies.orient.core.sql.query;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandResultListener;

/**
 * SQL synchronous query. When executed the caller wait for the result.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 * @see OSQLAsynchQuery
 */
@SuppressWarnings({ "unchecked", "serial" })
public class OSQLSynchQuery<T extends Object> extends OSQLAsynchQuery<T> implements OCommandResultListener {
	protected final List<T>	result	= new ArrayList<T>();

	public OSQLSynchQuery() {
		resultListener = this;
	}

	public OSQLSynchQuery(final String iText) {
		super(iText);
		resultListener = this;
	}

	public OSQLSynchQuery(final String iText, final int iLimit) {
		super(iText, iLimit, null);
		resultListener = this;
	}

	@Override
	public void reset() {
		result.clear();
	}

	public boolean result(final Object iRecord) {
		// database.callbackHooks(TYPE.BEFORE_READ, (OIdentifiable) iRecord);
		result.add((T) iRecord);
		// database.callbackHooks(TYPE.AFTER_READ, (OIdentifiable) iRecord);

		return true;
	}

	@Override
	public List<T> run(Object... iArgs) {
		if (!result.isEmpty()) {
			// HANDLE PAGINATION AUTOMATICALLY BY MOVING THE PAGE RANGE
			result.clear();
		}

		super.run(iArgs);
		return result;
	}

	public Object getResult() {
		return result;
	}
}
