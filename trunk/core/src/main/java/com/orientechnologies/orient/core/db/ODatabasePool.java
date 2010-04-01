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
package com.orientechnologies.orient.core.db;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;

public abstract class ODatabasePool<DB extends ODatabase> implements OResourcePoolListener<String, DB> {
	private static final int															WAIT_TIMEOUT	= 5000;
	private final Map<String, OResourcePool<String, DB>>	pools					= new HashMap<String, OResourcePool<String, DB>>();
	private int																						maxSize;

	public ODatabasePool(final int iMaxSize) {
		maxSize = iMaxSize;
	}

	public DB acquireDatabase(final String iURL) throws OLockException, InterruptedException {
		OResourcePool<String, DB> pool = pools.get(iURL);
		if (pool == null) {
			synchronized (pools) {
				if (pool == null) {
					pool = new OResourcePool<String, DB>(maxSize, this);
					pools.put(iURL, pool);
				}
			}
		}

		return pool.getResource(iURL, WAIT_TIMEOUT);
	}

	public void releaseDatabase(final String iURL, final DB iDatabase) {
		OResourcePool<String, DB> pool = pools.get(iURL);
		if (pool == null)
			throw new OLockException("Can't release a database URL not acquired before. URL: " + iURL);

		pool.returnResource(iDatabase);
	}
}
