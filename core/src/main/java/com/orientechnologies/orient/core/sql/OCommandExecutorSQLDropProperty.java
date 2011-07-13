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
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropProperty extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_DROP			= "DROP";
	public static final String	KEYWORD_PROPERTY	= "PROPERTY";

	private String							className;
	private String							fieldName;

	public OCommandExecutorSQLDropProperty parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_DELETE);

		init(iRequest.getDatabase(), iRequest.getText());

		final StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_PROPERTY + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		String[] parts = word.toString().split("\\.");
		if (parts.length != 2)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		className = parts[0];
		if (className == null)
			throw new OCommandSQLParsingException("Class not found", text, pos);
		fieldName = parts[1];

		return this;
	}

	/**
	 * Execute the CREATE PROPERTY.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (fieldName == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		final OClassImpl sourceClass = (OClassImpl) database.getMetadata().getSchema().getClass(className);
		if (sourceClass == null)
			throw new OCommandExecutionException("Source class '" + className + "' not found");

		// REMOVE THE PROPERTY
		sourceClass.dropPropertyInternal(fieldName);
		sourceClass.saveInternal();

		return null;
	}
}
