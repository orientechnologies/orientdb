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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL REMOVE INDEX command: Remove an index
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLDropIndex extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_DROP	= "DROP";
	public static final String	KEYWORD_INDEX	= "INDEX";

	private String							sourceClassName;
	private String							field;

	public OCommandExecutorSQLDropIndex parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		final StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_DROP))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_DROP + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_INDEX))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_INDEX + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		String[] parts = word.toString().split("\\.");
		if (parts.length != 2)
			throw new OCommandSQLParsingException("Expected <class>.<property>", text, pos);

		sourceClassName = parts[0];
		if (sourceClassName == null)
			throw new OCommandSQLParsingException("Class not found", text, pos);
		field = parts[1];

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1)
			return this;

		return this;
	}

	/**
	 * Execute the REMOVE INDEX.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (field == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		final OClass cls = database.getMetadata().getSchema().getClass(sourceClassName);
		if (cls == null)
			throw new OCommandExecutionException("Class '" + sourceClassName + "' not found");

		final OProperty prop = cls.getProperty(field);
		if (prop == null)
			throw new IllegalArgumentException("Property '" + field + "' was not found in class '" + cls + "'");

		if (prop.getIndex() == null)
			throw new IllegalArgumentException("Property '" + cls + "." + field + "' has not index");

		final long indexedItems = prop.getIndex().getUnderlying().size();

		prop.removeIndex();

		return indexedItems;
	}
}
