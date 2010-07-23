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

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL CREATE PROPERTY command: Creates a new property in the target class.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLCreateClass extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_CREATE	= "CREATE";
	public static final String	KEYWORD_CLASS		= "CLASS";

	private String							className;

	public OCommandExecutorSQLCreateClass parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CREATE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CREATE + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_CLASS))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_CLASS + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, false);
		if (pos == -1)
			throw new OCommandSQLParsingException("Expected <class>", text, pos);

		className = word.toString();
		if (className == null)
			throw new OCommandSQLParsingException("Class " + className + " already exists", text, pos);

		return this;
	}

	/**
	 * Execute the CREATE CLASS.
	 */
	public Object execute(final Object... iArgs) {
		if (className == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		OClass sourceClass = database.getMetadata().getSchema().getClass(className);
		if (sourceClass != null)
			throw new OCommandExecutionException("Class " + className + " already exists");

		sourceClass = database.getMetadata().getSchema().createClass(className);

		database.getMetadata().getSchema().save();

		return sourceClass.getId();
	}
}
