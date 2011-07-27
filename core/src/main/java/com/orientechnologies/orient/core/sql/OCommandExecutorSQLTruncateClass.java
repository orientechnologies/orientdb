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
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL TRUNCATE CLASS command: Truncates an entire class deleting all configured clusters where the class relies on.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLTruncateClass extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_TRUNCATE	= "TRUNCATE";
	public static final String	KEYWORD_CLASS			= "CLASS";
	private OClass							schemaClass;

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLTruncateClass parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_DELETE);
		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_TRUNCATE + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, text, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected class name", text, oldPos);

		final String className = word.toString();

		schemaClass = database.getMetadata().getSchema().getClass(className);

		if (schemaClass == null)
			throw new OCommandSQLParsingException("Class '" + className + "' not found", text, oldPos);
		return this;
	}

	/**
	 * Execute the command.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (schemaClass == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		final long recs = schemaClass.count();

		try {
			schemaClass.truncate();
		} catch (IOException e) {
			throw new OCommandExecutionException("Error on executing command", e);
		}

		return recs;
	}
}
