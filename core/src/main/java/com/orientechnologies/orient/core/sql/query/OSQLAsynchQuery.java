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

import com.orientechnologies.orient.core.command.OCommandRequestAsynch;
import com.orientechnologies.orient.core.command.OCommandResultListener;

/**
 * SQL asynchronous query. When executed the caller does not wait the the execution, rather the listener will be called foreach item
 * found in the query. OSQLAsynchQuery has been built on top of this.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 * @see OSQLSynchQuery
 */
public class OSQLAsynchQuery<T extends Object> extends OSQLQuery<T> implements OCommandRequestAsynch {
	protected int	resultCount	= 0;

	/**
	 * Empty constructor for unmarshalling.
	 */
	public OSQLAsynchQuery() {
	}

	public OSQLAsynchQuery(final String iText) {
		this(iText, null);
	}

	public OSQLAsynchQuery(final String iText, final OCommandResultListener iResultListener) {
		this(iText, -1, iResultListener);
	}

	public OSQLAsynchQuery(final String iText, final int iLimit, final OCommandResultListener iResultListener) {
		super(iText);
		limit = iLimit;
		resultListener = iResultListener;
	}

	@SuppressWarnings("unchecked")
	public <RET> RET execute2(final String iText, final Object... iArgs) {
		text = iText;
		return (RET) execute(iArgs);
	}

	public T executeFirst() {
		execute(1);
		return null;
	}
}
