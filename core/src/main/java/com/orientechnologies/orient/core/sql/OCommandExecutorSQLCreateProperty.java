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

import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL CREATE LINK command: Transform a JOIN relationship to a physical LINK
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateProperty extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_CREATE		= "CREATE";
	public static final String	KEYWORD_PROPERTY	= "PROPERTY";

	private String							className;
	private String							fieldName;
	private OType								type;
	private String							linked;

	public OCommandExecutorSQLCreateProperty parse(final OCommandRequestInternal iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found", text, oldPos);

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

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Missed property type", text, oldPos);

		type = OType.valueOf(word.toString());

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
		if (pos == -1)
			return this;

		linked = word.toString();

		return this;
	}

	/**
	 * Execute the GRANT.
	 */
	public Object execute(final Object... iArgs) {
		if (type == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		OClass sourceClass = database.getMetadata().getSchema().getClass(className);
		if (sourceClass == null)
			throw new OCommandExecutionException("Source class '" + className + "' not found");

		OProperty prop = sourceClass.getProperty(fieldName);
		if (prop != null)
			throw new OCommandExecutionException("Property '" + className + "." + fieldName
					+ "' already exists. Remove it before to retry.");

		// CREATE THE PROPERTY
		OClass linkedClass = null;
		OType linkedType = null;
		if (linked != null) {
			// FIRST SEARCH BETWEEN CLASSES
			linkedClass = database.getMetadata().getSchema().getClass(linked);

			if (linkedClass == null)
				// NOT FOUND: SEARCH BETWEEN TYPES
				linkedType = OType.valueOf(linked);
		}

		if (linkedClass != null)
			sourceClass.createProperty(fieldName, type, linkedClass);
		else if (linkedType != null)
			sourceClass.createProperty(fieldName, type, linkedType);
		else
			sourceClass.createProperty(fieldName, type);

		database.getMetadata().getSchema().save();

		return 1;
	}
}
