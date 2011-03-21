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
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.geo.OSQLFunctionDistance;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionAverage;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMax;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMin;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionSum;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionFormat;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionSysdate;

public class OSQLEngine {
	private Map<String, OSQLFunction>																	inlineFunctions				= new HashMap<String, OSQLFunction>();
	private Map<String, Class<? extends OSQLFunction>>								aggregationFunctions	= new HashMap<String, Class<? extends OSQLFunction>>();
	private Map<String, Class<? extends OCommandExecutorSQLAbstract>>	commands							= new HashMap<String, Class<? extends OCommandExecutorSQLAbstract>>();

	private static final OSQLEngine																		INSTANCE							= new OSQLEngine();

	protected OSQLEngine() {
		// COMMANDS
		commands.put(OCommandExecutorSQLSelect.KEYWORD_SELECT, OCommandExecutorSQLSelect.class);
		commands.put(OCommandExecutorSQLInsert.KEYWORD_INSERT, OCommandExecutorSQLInsert.class);
		commands.put(OCommandExecutorSQLUpdate.KEYWORD_UPDATE, OCommandExecutorSQLUpdate.class);
		commands.put(OCommandExecutorSQLDelete.KEYWORD_DELETE, OCommandExecutorSQLDelete.class);
		commands.put(OCommandExecutorSQLGrant.KEYWORD_GRANT, OCommandExecutorSQLGrant.class);
		commands.put(OCommandExecutorSQLRevoke.KEYWORD_REVOKE, OCommandExecutorSQLRevoke.class);
		commands.put(OCommandExecutorSQLCreateLink.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateLink.KEYWORD_LINK,
				OCommandExecutorSQLCreateLink.class);
		commands.put(OCommandExecutorSQLCreateIndex.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateIndex.KEYWORD_INDEX,
				OCommandExecutorSQLCreateIndex.class);
		commands.put(OCommandExecutorSQLRemoveIndex.KEYWORD_REMOVE + " " + OCommandExecutorSQLRemoveIndex.KEYWORD_INDEX,
				OCommandExecutorSQLRemoveIndex.class);
		commands.put(OCommandExecutorSQLCreateClass.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateClass.KEYWORD_CLASS,
				OCommandExecutorSQLCreateClass.class);
		commands.put(OCommandExecutorSQLCreateProperty.KEYWORD_CREATE + " " + OCommandExecutorSQLCreateProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLCreateProperty.class);
		commands.put(OCommandExecutorSQLRemoveClass.KEYWORD_REMOVE + " " + OCommandExecutorSQLRemoveClass.KEYWORD_CLASS,
				OCommandExecutorSQLRemoveClass.class);
		commands.put(OCommandExecutorSQLRemoveProperty.KEYWORD_REMOVE + " " + OCommandExecutorSQLRemoveProperty.KEYWORD_PROPERTY,
				OCommandExecutorSQLRemoveProperty.class);
		commands.put(OCommandExecutorSQLFindReferences.KEYWORD_FIND + " " + OCommandExecutorSQLFindReferences.KEYWORD_REFERENCES,
				OCommandExecutorSQLFindReferences.class);

		// MISC FUNCTIONS
		registerFunction(OSQLFunctionFormat.NAME, new OSQLFunctionFormat());
		registerFunction(OSQLFunctionSysdate.NAME, OSQLFunctionSysdate.class);
		registerFunction(OSQLFunctionCount.NAME, OSQLFunctionCount.class);

		// MATH FUNCTIONS
		registerFunction(OSQLFunctionMin.NAME, OSQLFunctionMin.class);
		registerFunction(OSQLFunctionMax.NAME, OSQLFunctionMax.class);
		registerFunction(OSQLFunctionSum.NAME, OSQLFunctionSum.class);
		registerFunction(OSQLFunctionAverage.NAME, OSQLFunctionAverage.class);

		// GEO FUNCTIONS
		registerFunction(OSQLFunctionDistance.NAME, new OSQLFunctionDistance());
	}

	public void registerFunction(final String iName, final OSQLFunction iFunction) {
		inlineFunctions.put(iName.toUpperCase(), iFunction);
	}

	public void registerFunction(final String iName, final Class<? extends OSQLFunction> iFunctionClass) {
		aggregationFunctions.put(iName.toUpperCase(), iFunctionClass);
	}

	public void unregisterInlineFunction(final String iName) {
		inlineFunctions.remove(iName.toUpperCase());
	}

	public OSQLFunction getInlineFunction(final String iFunctionName) {
		return inlineFunctions.get(iFunctionName.toUpperCase());
	}

	public OSQLFunction getAggregationFunction(final String iFunctionName) {
		final Class<? extends OSQLFunction> f = aggregationFunctions.get(iFunctionName.toUpperCase());
		if (f != null)
			try {
				return f.newInstance();
			} catch (Exception e) {
				throw new OCommandExecutionException("Error in creation of function " + iFunctionName
						+ "(). Probably there is not an empty constructor or the constructor generates errors", e);
			}
		return null;
	}

	public void unregisterFunction(final String iName) {
		if (inlineFunctions.remove(iName.toUpperCase()) == null)
			aggregationFunctions.remove(iName.toUpperCase());
	}

	public OCommandExecutorSQLAbstract getCommand(final String iText) {
		int pos = -1;
		Class<? extends OCommandExecutorSQLAbstract> commandClass = null;
		while (commandClass == null) {
			pos = iText.indexOf(' ', pos + 1);
			if (pos > -1) {
				String piece = iText.substring(0, pos);
				commandClass = commands.get(piece);
			} else
				break;
		}

		if (commandClass != null)
			try {
				return commandClass.newInstance();
			} catch (Exception e) {
				throw new OCommandExecutionException("Error in creation of command " + commandClass
						+ "(). Probably there is not an empty constructor or the constructor generates errors", e);
			}

		return null;
	}

	public OSQLFilter parseWhereCondition(final ODatabaseRecord iDatabase, final String iText) {
		return new OSQLFilter(iDatabase, iText);
	}

	public static OSQLEngine getInstance() {
		return INSTANCE;
	}
}
