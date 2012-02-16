/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;

public class OTraverseContext implements OCommandContext {
	public Set<ORID>			traversed	= new HashSet<ORID>();
	public List<ORID>			history		= new ArrayList<ORID>();
	public OTraversePath	path			= new OTraversePath();

	@SuppressWarnings("serial")
	public class OTraversePath extends ArrayList<String> {
		@Override
		public String toString() {
			final StringBuilder buffer = new StringBuilder();
			for (String item : this) {
				if (buffer.length() > 0)
					buffer.append('.');
				buffer.append(item);
			}
			return buffer.toString();
		}
	}

	public Object getVariable(final String iName) {
		if ("depth".equalsIgnoreCase(iName))
			return history.size();
		else if ("path".equalsIgnoreCase(iName))
			return path;
		else if ("history".equalsIgnoreCase(iName))
			return history;
		return null;
	}

	public void setVariable(final String iName, final Object iValue) {
		if ("depth".equalsIgnoreCase(iName))
			throw new OCommandExecutionException("Cannot change read-only 'depth' variable. Current value is: " + history.size());
	}

	public Map<String, Object> getVariables() {
		final HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("depth", history.size());
		map.put("path", path);
		map.put("history", history);
		return map;
	}

	public void merge(OCommandContext context) {
	}

	@Override
	public String toString() {
		return getVariables().toString();
	}
}