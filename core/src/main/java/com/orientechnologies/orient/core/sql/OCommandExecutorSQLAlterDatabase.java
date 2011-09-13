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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL ALTER DATABASE command: Changes an attribute of the current database.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterDatabase extends OCommandExecutorSQLPermissionAbstract {
	public static final String		KEYWORD_ALTER			= "ALTER";
	public static final String		KEYWORD_DATABASE	= "DATABASE";

	private ODatabase.ATTRIBUTES	attribute;
	private String								value;

	public OCommandExecutorSQLAlterDatabase parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_UPDATE);

		init(iRequest.getDatabase(), iRequest.getText());

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_ALTER))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_ALTER + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_DATABASE))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_DATABASE + " not found", text, oldPos);

		oldPos = pos;
		pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Missed the database's attribute to change", text, oldPos);

		final String attributeAsString = word.toString();

		try {
			attribute = ODatabase.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
		} catch (IllegalArgumentException e) {
			throw new OCommandSQLParsingException("Unknown database's attribute '" + attributeAsString + "'. Supported attributes are: "
					+ Arrays.toString(OClass.ATTRIBUTES.values()), text, oldPos);
		}

		value = text.substring(pos + 1).trim();

		if (value.length() == 0)
			throw new OCommandSQLParsingException("Missed the database's value to change for attribute '" + attribute + "'", text, oldPos);

		return this;
	}

	/**
	 * Execute the ALTER DATABASE.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (attribute == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		database.checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);

		((ODatabaseComplex<?>) database).setInternal(attribute, value);
		return null;
	}
}
