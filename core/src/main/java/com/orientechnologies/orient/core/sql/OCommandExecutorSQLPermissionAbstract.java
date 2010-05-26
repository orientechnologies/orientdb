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

import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * SQL GRANT command: Grant a privilege to a database role.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OCommandExecutorSQLPermissionAbstract extends OCommandExecutorSQLAbstract {
	protected static final String	KEYWORD_ON	= "ON";
	protected int									privilege;
	protected String							resource;
	protected ORole								role;

	protected void parsePrivilege(final StringBuilder word, final int oldPos) {
		final String privilegeName = word.toString();

		if ("CREATE".equals(privilegeName))
			privilege = ORole.PERMISSION_CREATE;
		else if ("READ".equals(privilegeName))
			privilege = ORole.PERMISSION_READ;
		else if ("UPDATE".equals(privilegeName))
			privilege = ORole.PERMISSION_UPDATE;
		else if ("DELETE".equals(privilegeName))
			privilege = ORole.PERMISSION_DELETE;
		else if ("ALL".equals(privilegeName))
			privilege = ORole.PERMISSION_ALL;
		else
			throw new OCommandSQLParsingException("Unrecognized privilege '" + privilegeName + "'", text, oldPos);
	}

}
