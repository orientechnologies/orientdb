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
package com.orientechnologies.orient.core.command;

import java.util.HashMap;
import java.util.Map;

/**
 * Basic implementation of OCommandContext interface that stores variables in a map.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OBasicCommandContext implements OCommandContext {
	private OCommandContext			inherited;
	private Map<String, Object>	variables;

	public Object getVariable(final String iName) {
		Object result = inherited != null ? inherited.getVariable(iName) : null;
		if (variables != null && variables.containsKey(iName))
			result = variables.get(iName);
		return result;
	}

	public void setVariable(final String iName, final Object iValue) {
		init();
		variables.put(iName, iValue);
	}

	/**
	 * Returns a read-only map with all the variables.
	 */
	public Map<String, Object> getVariables() {
		final HashMap<String, Object> map = new HashMap<String, Object>();
		if (inherited != null)
			map.putAll(inherited.getVariables());
		if (variables != null)
			map.putAll(variables);
		return map;
	}

	/**
	 * Set the inherited context avoiding to copy all the values every time.
	 */
	public void merge(final OCommandContext iContext) {
		if (iContext == null)
			return;

		inherited = iContext;
	}

	@Override
	public String toString() {
		return getVariables().toString();
	}

	private void init() {
		if (variables == null)
			variables = new HashMap<String, Object>();
	}
}
