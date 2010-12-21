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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.impl.OSQLFunctionDistance;

public class OSQLEngine {
	private Map<String, OSQLFunction>	functions	= new HashMap<String, OSQLFunction>();

	private static final OSQLEngine		INSTANCE	= new OSQLEngine();

	protected OSQLEngine() {
		registerFunction(OSQLFunctionDistance.NAME, new OSQLFunctionDistance());
	}

	public OSQLFunction getFunction(final String iFunctionName) {
		final OSQLFunction f = functions.get(iFunctionName);
		if (f != null)
			return f;

		throw new IllegalArgumentException("Unknow function " + iFunctionName + "()");
	}

	public void registerFunction(final String iName, final OSQLFunction iFunction) {
		functions.put(iName.toUpperCase(), iFunction);
	}

	public void unregisterFunction(final String iName) {
		functions.remove(iName.toUpperCase());
	}

	public OSQLFilter parseWhereCondition(final ODatabaseRecord<?> iDatabase, final String iText) {
		return new OSQLFilter(iDatabase, iText);
	}

	public static OSQLEngine getInstance() {
		return INSTANCE;
	}
}
