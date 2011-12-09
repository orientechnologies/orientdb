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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.storage.OCluster;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropClass extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_DROP	= "DROP";
	public static final String	KEYWORD_CLASS	= "CLASS";

	private String							className;

	public OCommandExecutorSQLDropClass parse(final OCommandRequestText iRequest) {
		getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_DELETE);

		init(iRequest.getText());

		final StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>", text, pos);

		className = word.toString();
		if (className == null)
			throw new OCommandSQLParsingException("Class is null", text, pos);

		return this;
	}

	/**
	 * Execute the DROP CLASS.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (className == null)
			throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

		final ODatabaseRecord database = getDatabase();
		final OClass oClass = database.getMetadata().getSchema().getClass(className);
		if (oClass == null)
			return null;

		for (final OIndex oIndex : oClass.getClassIndexes()) {
			database.getMetadata().getIndexManager().dropIndex(oIndex.getName());
		}

		final int clusterId = oClass.getDefaultClusterId();

		((OSchemaProxy) database.getMetadata().getSchema()).dropClassInternal(className);
		((OSchemaProxy) database.getMetadata().getSchema()).saveInternal();

		deleteDefaultCluster(clusterId);

		return null;
	}

	protected void deleteDefaultCluster(int clusterId) {
		final ODatabaseRecord database = getDatabase();
		OCluster cluster = database.getStorage().getClusterById(clusterId);
		if (cluster.getName().equalsIgnoreCase(className)) {
			if (isClusterDeletable(clusterId)) {
				database.getStorage().dropCluster(clusterId);
			}
		}
	}

	protected boolean isClusterDeletable(int clusterId) {
		final ODatabaseRecord database = getDatabase();
		for (OClass iClass : database.getMetadata().getSchema().getClasses()) {
			for (int i : iClass.getClusterIds()) {
				if (i == clusterId)
					return false;
			}
		}
		return true;
	}
}
