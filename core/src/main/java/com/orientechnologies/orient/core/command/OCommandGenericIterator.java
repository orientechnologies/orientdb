/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.Collection;
import java.util.Iterator;

/**
 * Iterator for commands. Allows to iterate against the resultset of the execution.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandGenericIterator implements Iterator<Object>, Iterable<Object> {
	protected OCommandExecutor	command;
	protected Iterator<Object>	resultSet;
	protected Object						resultOne;
	protected boolean						executed	= false;

	public OCommandGenericIterator(OCommandExecutor command) {
		this.command = command;
	}

	public boolean hasNext() {
		checkForExecution();
		if (resultOne != null)
			return true;
		else if (resultSet != null)
			return resultSet.hasNext();
		return false;
	}

	public Object next() {
		checkForExecution();
		if (resultOne != null)
			return resultOne;
		else if (resultSet != null)
			return resultSet.next();

		return null;
	}

	public Iterator<Object> iterator() {
		return this;
	}

	public void remove() {
		throw new UnsupportedOperationException("remove()");
	}

	@SuppressWarnings("unchecked")
	protected void checkForExecution() {
		if (!executed) {
			executed = true;
			final Object result = command.execute(null);
			if (result instanceof Collection)
				resultSet = ((Collection<Object>) result).iterator();
			else if (result instanceof Object)
				resultOne = (Object) result;
		}
	}
}
