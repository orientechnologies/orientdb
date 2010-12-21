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
package com.orientechnologies.orient.core.sql.filter;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionDistance;

public class OSQLParser {
	private Map<String, Class<? extends OSQLFunction>>	functions	= new HashMap<String, Class<? extends OSQLFunction>>();

	private static final OSQLParser											INSTANCE	= new OSQLParser();

	protected OSQLParser() {
		registerFunction(OSQLFunctionDistance.NAME, OSQLFunctionDistance.class);
	}

	public Class<? extends OSQLFunction> getFunction(final String iFunctionName) {
		return functions.get(iFunctionName);
	}

	private void registerFunction(final String iName, final Class<? extends OSQLFunction> iFunction) {
		functions.put(iName.toUpperCase(), iFunction);
	}

	public OSQLFilter parseWhereCondition(final ODatabaseRecord<?> iDatabase, final String iText) {
		return new OSQLFilter(iDatabase, iText);
	}

	public static OSQLParser getInstance() {
		return INSTANCE;
	}
}
