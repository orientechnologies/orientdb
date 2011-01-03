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

import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLDelete extends OCommandExecutorSQLAbstract implements OCommandResultListener {
	public static final String		KEYWORD_DELETE	= "DELETE";

	private OSQLQuery<ODocument>	query;
	private int										recordCount			= 0;

	public OCommandExecutorSQLDelete() {
	}

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLDelete parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_DELETE);

		init(iRequest.getDatabase(), iRequest.getText());

		query = null;
		recordCount = 0;

		StringBuilder word = new StringBuilder();

		int pos = OSQLHelper.nextWord(text, textUpperCase, 0, word, true);
		if (pos == -1 || !word.toString().equals(OCommandExecutorSQLDelete.KEYWORD_DELETE))
			throw new OCommandSQLParsingException("Keyword " + OCommandExecutorSQLDelete.KEYWORD_DELETE + " not found", text, 0);

		query = database.command(new OSQLAsynchQuery<ODocument>("select " + text.substring(pos), this));

		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		if (query == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		query.execute(iArgs);
		return recordCount;
	}

	/**
	 * Delete the current record.
	 */
	@SuppressWarnings("unchecked")
	public boolean result(final Object iRecord) {
		((ORecordAbstract<Object>) iRecord).delete();
		recordCount++;
		return true;
	}
}
