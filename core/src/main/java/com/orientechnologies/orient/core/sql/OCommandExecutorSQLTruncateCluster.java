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

import java.io.IOException;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

/**
 * SQL TRUNCATE CLUSTER command: Truncates an entire record cluster.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLTruncateCluster extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_TRUNCATE	= "TRUNCATE";
	public static final String	KEYWORD_CLUSTER		= "CLUSTER";
	private String							clusterName;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLTruncateCluster parse(final OCommandRequestText iRequest) {
		final ODatabaseRecord database = getDatabase();
		database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_DELETE);
		init(iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_TRUNCATE + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CLUSTER))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLUSTER + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, text, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected cluster name", text, oldPos);

		clusterName = word.toString();

		if (database.getClusterIdByName(clusterName) == -1)
			throw new OCommandSQLParsingException("Cluster '" + clusterName + "' not found", text, oldPos);
		return this;
	}

	/**
	 * Execute the command.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (clusterName == null)
			throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

		OCluster cluster = ((OStorageEmbedded) getDatabase().getStorage()).getClusterByName(clusterName);

		final long recs = cluster.getEntries();

		try {
			cluster.truncate();
		} catch (IOException e) {
			throw new OCommandExecutionException("Error on executing command", e);
		}

		return recs;
	}
}
