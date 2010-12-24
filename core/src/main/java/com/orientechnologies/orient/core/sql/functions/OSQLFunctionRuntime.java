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

import com.orientechnologies.common.types.ORef;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;

/**
 * Represents a state-less function at run-time.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionRuntime {

	public OSQLFunction	function;
	public Object[]			configuredParameters;
	public Object[]			runtimeParameters;
	public ORef<Object>	context;

	public OSQLFunctionRuntime(final OSQLFunction function, final Object[] configuredParameters) {
		this.function = function;
		this.configuredParameters = configuredParameters;

		// COPY STATIC VALUES
		this.runtimeParameters = new Object[configuredParameters.length];
		for (int i = 0; i < configuredParameters.length; ++i) {
			if (!(configuredParameters[i] instanceof OSQLFilterItemField) && !(configuredParameters[i] instanceof OSQLFunctionRuntime))
				runtimeParameters[i] = configuredParameters[i];
		}

		this.context = new ORef<Object>();
	}

	/**
	 * Execute a function.
	 * 
	 * @param iRecord
	 *          Current record
	 * @return
	 */
	public Object execute(ORecordSchemaAware<?> iRecord) {
		// RESOLVE VALUES USING THE CURRENT RECORD
		for (int i = 0; i < configuredParameters.length; ++i) {
			if (configuredParameters[i] instanceof OSQLFilterItemField)
				runtimeParameters[i] = ((OSQLFilterItemField) configuredParameters[i]).getValue(iRecord);
			else if (configuredParameters[i] instanceof OSQLFunctionRuntime)
				runtimeParameters[i] = ((OSQLFunctionRuntime) configuredParameters[i]).execute(iRecord);
		}

		return function.execute(context, runtimeParameters);
	}
}
