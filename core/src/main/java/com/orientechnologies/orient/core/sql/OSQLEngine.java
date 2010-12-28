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
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;

public class OSQLEngine {
	private Map<String, OSQLFunction>										inlineFunctions				= new HashMap<String, OSQLFunction>();
	private Map<String, Class<? extends OSQLFunction>>	aggregationFunctions	= new HashMap<String, Class<? extends OSQLFunction>>();

	private static final OSQLEngine											INSTANCE							= new OSQLEngine();

	protected OSQLEngine() {
		// MISC FUNCTIONS
		registerFunction(OSQLFunctionCount.NAME, OSQLFunctionCount.class);

		// MATH FUNCTIONS
		registerFunction(OSQLFunctionMax.NAME, OSQLFunctionMax.class);
		registerFunction(OSQLFunctionMin.NAME, OSQLFunctionMin.class);
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

	public OSQLFilter parseWhereCondition(final ODatabaseRecord<?> iDatabase, final String iText) {
		return new OSQLFilter(iDatabase, iText);
	}

	public static OSQLEngine getInstance() {
		return INSTANCE;
	}
}
