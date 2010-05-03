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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * SQL command implementation.
 * 
 * @author luca
 * 
 */
public abstract class OCommandExecutorSQLAbstract implements OCommandExecutor<ODatabaseRecord<?>> {
	protected String							text;
	protected String							textUpperCase;
	protected ODatabaseRecord<?>	database;

	public OCommandExecutorSQLAbstract init(final ODatabaseRecord<?> iDatabase, final String iText) {
		database = iDatabase;
		text = iText;
		textUpperCase = iText.toUpperCase();
		return this;
	}

	/**
	 * Parse every time the request and execute it.
	 */
	public Object execute(final OCommandRequestInternal<ODatabaseRecord<?>> iRequest, final Object... iArgs) {
		parse(iRequest);
		return execute(iArgs);
	}

	public String getText() {
		return text;
	}

	public ODatabaseRecord<?> getDatabase() {
		return database;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [text=" + text + "]";
	}
}
