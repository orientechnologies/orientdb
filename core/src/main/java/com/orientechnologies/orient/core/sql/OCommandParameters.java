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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Container of arguments. Manages also the ordinal arguments.
 * 
 * @author Luca Garulli
 * 
 * @param <K>
 * @param <V>
 */
public class OCommandParameters implements Iterable<Map.Entry<Object, Object>> {
	private final Map<Object, Object>	parameters;
	private int												counter	= 0;

	public OCommandParameters() {
		parameters = new HashMap<Object, Object>();
	}

	public OCommandParameters(final Map<Object, Object> iArgs) {
		if (iArgs != null)
			parameters = iArgs;
		else
			parameters = new HashMap<Object, Object>();
	}

	public void putArgument(final Object k, final Object v) {
		parameters.put(k, v);
	}

	public Object getByName(final Object iName) {
		return parameters.get(iName);
	}

	public Object getNext() {
		if (parameters.size() <= counter)
			throw new IndexOutOfBoundsException("No enough parameters are passed. You requested parameter " + counter + " but only "
					+ parameters.size() + " was found");

		return parameters.get(counter++);
	}

	public Iterator<Entry<Object, Object>> iterator() {
		return parameters.entrySet().iterator();
	}

	public int size() {
		return parameters.size();
	}

	public void reset() {
		counter = 0;
	}
}
