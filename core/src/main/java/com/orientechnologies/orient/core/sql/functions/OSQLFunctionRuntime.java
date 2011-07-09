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
package com.orientechnologies.orient.core.sql.functions;

import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * Wraps functions managing the binding of parameters.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionRuntime {

	public OSQLFunction	function;
	public Object[]			configuredParameters;
	public Object[]			runtimeParameters;

	public OSQLFunctionRuntime(final OSQLFunction function, final Object[] configuredParameters) {
		this.function = function;
		this.configuredParameters = configuredParameters;

		// COPY STATIC VALUES
		this.runtimeParameters = new Object[configuredParameters.length];
		for (int i = 0; i < configuredParameters.length; ++i) {
			if (!(configuredParameters[i] instanceof OSQLFilterItemField) && !(configuredParameters[i] instanceof OSQLFunctionRuntime))
				runtimeParameters[i] = configuredParameters[i];
		}
	}

	public boolean aggregateResults() {
		return function.aggregateResults(configuredParameters);
	}

	public boolean filterResult() {
		return function.filterResult();
	}

	/**
	 * Execute a function.
	 * 
	 * @param iRecord
	 *          Current record
	 * @return
	 */
	public Object execute(final ORecordSchemaAware<?> iRecord) {
		// RESOLVE VALUES USING THE CURRENT RECORD
		for (int i = 0; i < configuredParameters.length; ++i) {
			if (configuredParameters[i] instanceof OSQLFilterItemField)
				runtimeParameters[i] = ((OSQLFilterItemField) configuredParameters[i]).getValue(iRecord);
			else if (configuredParameters[i] instanceof OSQLFunctionRuntime)
				runtimeParameters[i] = ((OSQLFunctionRuntime) configuredParameters[i]).execute(iRecord);
		}

		return function.execute(iRecord, runtimeParameters);
	}

	public Object getResult() {
		return function.getResult();
	}

	public void setResult(final Object iValue) {
		function.setResult(iValue);
	}
}
