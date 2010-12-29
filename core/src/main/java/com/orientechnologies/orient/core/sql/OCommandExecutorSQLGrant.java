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
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLGrant extends OCommandExecutorSQLPermissionAbstract {
	public static final String	KEYWORD_GRANT	= "GRANT";
	private static final String	KEYWORD_TO		= "TO";

	@SuppressWarnings("unchecked")
	public OCommandExecutorSQLGrant parse(final OCommandRequestText iRequest) {
		iRequest.getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_CREATE);

		init(iRequest.getDatabase(), iRequest.getText());

		privilege = ORole.PERMISSION_NONE;
		resource = null;
		role = null;

		StringBuilder word = new StringBuilder();

		int oldPos = 0;
		int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_GRANT))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_GRANT + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Invalid privilege", text, oldPos);

		parsePrivilege(word, oldPos);

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_ON))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_ON + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, text, pos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Invalid resource", text, oldPos);

		resource = word.toString();

		pos = OSQLHelper.nextWord(text, textUpperCase, pos, word, true);
		if (pos == -1 || !word.toString().equals(KEYWORD_TO))
			throw new OCommandSQLParsingException("Keyword " + KEYWORD_TO + " not found", text, oldPos);

		pos = OSQLHelper.nextWord(text, text, pos, word, true);
		if (pos == -1)
			throw new OCommandSQLParsingException("Invalid role", text, oldPos);

		final String roleName = word.toString();
		role = database.getMetadata().getSecurity().getRole(roleName);
		if (role == null)
			throw new OCommandSQLParsingException("Invalid role: " + roleName);
		return this;
	}

	/**
	 * Execute the GRANT.
	 */
	public Object execute(final Map<Object, Object> iArgs) {
		if (role == null)
			throw new OCommandExecutionException("Can't execute the command because it hasn't been parsed yet");

		role.grant(resource, privilege);
		role.save();

		return role;
	}
}
