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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterCondition;
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
	private static final String		VALUE_NOT_FOUND	= "_not_found_";

	private OSQLQuery<ODocument>	query;
	private String								indexName				= null;
	private int										recordCount			= 0;

	private OSQLFilter						compiledFilter;

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

		int oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_FROM))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_FROM + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Invalid subject name. Expected cluster, class or index", text, oldPos);

		final String subjectName = word.toString();

		if (subjectName.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX)) {
			// INDEX
			indexName = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());
			compiledFilter = OSQLEngine.getInstance().parseFromWhereCondition(iRequest.getDatabase(), text.substring(oldPos));
		} else {
			query = database.command(new OSQLAsynchQuery<ODocument>("select from " + subjectName + " " + text.substring(pos), this));
		}

		return this;
	}

	public Object execute(final Map<Object, Object> iArgs) {
		if (query == null && indexName == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		if (query != null) {
			// AGAINST CLUSTERS AND CLASSES
			query.execute(iArgs);
			return recordCount;
		} else {
			// AGAINST INDEXES
			final OIndex<?> index = database.getMetadata().getIndexManager().getIndex(indexName);
			if (index == null)
				throw new OCommandExecutionException("Target index '" + indexName + "' not found");

			Object key = null;
			Object value = VALUE_NOT_FOUND;

			if (compiledFilter.getRootCondition() == null) {
				final long total = index.getSize();
				index.clear();
				return total;
			} else {
				if (KEYWORD_KEY.equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString()))
					// FOUND KEY ONLY
					key = compiledFilter.getRootCondition().getRight();
				else if (compiledFilter.getRootCondition().getLeft() instanceof OSQLFilterCondition) {
					// KEY AND VALUE
					final OSQLFilterCondition leftCondition = (OSQLFilterCondition) compiledFilter.getRootCondition().getLeft();
					if (KEYWORD_KEY.equalsIgnoreCase(leftCondition.getLeft().toString()))
						key = leftCondition.getRight();

					final OSQLFilterCondition rightCondition = (OSQLFilterCondition) compiledFilter.getRootCondition().getRight();
					if (KEYWORD_RID.equalsIgnoreCase(rightCondition.getLeft().toString()))
						value = rightCondition.getRight();

				}

				if (key == null)
					throw new OCommandExecutionException("'Key' field is required for queries against indexes");

				final boolean result;
				if (value != VALUE_NOT_FOUND)
					result = index.remove(key, (OIdentifiable) value);
				else
					result = index.remove(key);
				return result ? 1 : 0;
			}
		}
	}

	/**
	 * Delete the current record.
	 */
	public boolean result(final Object iRecord) {
		final ORecordAbstract<?> record = (ORecordAbstract<?>) iRecord;
		record.setDatabase(database);

		record.delete();

		recordCount++;
		return true;
	}
}
