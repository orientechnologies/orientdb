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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.security.OUser;

public abstract class ODatabasePool<DB extends ODatabaseRecord<?>> implements OResourcePoolListener<String, DB> {

	private static final int															DEF_WAIT_TIMEOUT	= 5000;
	private final Map<String, OResourcePool<String, DB>>	pools							= new HashMap<String, OResourcePool<String, DB>>();
	private int																						maxSize;
	private int																						timeout						= DEF_WAIT_TIMEOUT;

	public ODatabasePool(final int iMaxSize) {
		maxSize = iMaxSize;
	}

	public ODatabasePool(final int iMaxSize, final int iTimeout) {
		maxSize = iMaxSize;
		timeout = iTimeout;
	}

	public DB acquireDatabase(final String iURL) throws OLockException, InterruptedException {
		final String url = iURL.indexOf(":") > -1 ? iURL.substring(0, iURL.lastIndexOf(":")) : iURL;

		OResourcePool<String, DB> pool = pools.get(url);
		if (pool == null) {
			synchronized (pools) {
				if (pool == null) {
					pool = new OResourcePool<String, DB>(maxSize, this);
					pools.put(url, pool);
				}
			}
		}

		return pool.getResource(iURL, timeout);
	}

	public void releaseDatabase(final String iURL, final DB iDatabase) {
		if (iDatabase instanceof ODatabaseRecord<?>)
			((ODatabaseRecord<?>) iDatabase).rollback();

		OResourcePool<String, DB> pool = pools.get(iURL);
		if (pool == null)
			throw new OLockException("Can't release a database URL not acquired before. URL: " + iURL);

		pool.returnResource(iDatabase);
	}

	public DB reuseResource(String iKey, DB iValue) {
		final String[] parts = iKey.split(":");

		if (parts.length < 3)
			throw new IllegalArgumentException("Missed user or password");

		final OUser user = iValue.getMetadata().getSecurity().getUser(parts[1]);
		if (!user.checkPassword(parts[2]))
			throw new IllegalArgumentException("Wrong user/password");

		return null;

	}

	public Map<String, OResourcePool<String, DB>> getPools() {
		return pools;
	}
}
