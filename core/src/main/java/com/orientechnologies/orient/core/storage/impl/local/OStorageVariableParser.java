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
package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;

public class OStorageVariableParser implements OVariableParserListener {
	public static final String	STORAGE_PATH			= "STORAGE_PATH";
	private String							dbPath;
	public static final String	VAR_BEGIN					= "${";
	public static final String	VAR_END						= "}";
	public static final String	DB_PATH_VARIABLE	= VAR_BEGIN + STORAGE_PATH + VAR_END;

	public OStorageVariableParser(String dbPath) {
		this.dbPath = dbPath;
	}

	public String resolveVariables(String iPath) {
		return OVariableParser.resolveVariables(iPath, VAR_BEGIN, VAR_END, this);
	}

	public String convertPathToRelative(String iPath) {
		return iPath.replace(dbPath, VAR_BEGIN + STORAGE_PATH + VAR_END);
	}

	public String resolve(String variable) {
		if (variable.equals(STORAGE_PATH))
			return dbPath;

		String resolved = System.getProperty(variable);

		if (resolved == null)
			// TRY TO FIND THE VARIABLE BETWEEN SYSTEM'S ENVIRONMENT PROPERTIES
			resolved = System.getenv(variable);

		return resolved;
	}
}
