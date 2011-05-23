/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.common.concur.resource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import com.orientechnologies.common.exception.OException;

/**
 * Shared container that works with callbacks like closures.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class OSharedContainerImpl implements OSharedContainer {
	protected Map<String, Object>	sharedResources	= new HashMap<String, Object>();

	public synchronized boolean existsResource(final String iName) {
		return sharedResources.containsKey(iName);
	}

	public synchronized <T> T removeResource(final String iName) {
		return (T) sharedResources.remove(iName);
	}

	public synchronized <T> T getResource(final String iName, final Callable<T> iCallback) {
		T value = (T) sharedResources.get(iName);
		if (value == null) {
			// CREATE IT
			try {
				value = iCallback.call();
			} catch (Exception e) {
				throw new OException("Error on creation of shared resource", e);
			}
			sharedResources.put(iName, value);
		}

		return value;
	}
}
