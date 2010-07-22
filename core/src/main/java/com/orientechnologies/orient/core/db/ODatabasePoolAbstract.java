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

public abstract class ODatabasePoolAbstract<DB extends ODatabase> implements OResourcePoolListener<String, DB> {

	private static final int															DEF_WAIT_TIMEOUT	= 5000;
	private final Map<String, OResourcePool<String, DB>>	pools							= new HashMap<String, OResourcePool<String, DB>>();
	private int																						maxSize;
	private int																						timeout						= DEF_WAIT_TIMEOUT;
	private boolean																				enableSecurity		= true;

	public ODatabasePoolAbstract(final int iMinSize, final int iMaxSize, boolean iEnableSecurity) {
		this(iMinSize, iMaxSize, iEnableSecurity, DEF_WAIT_TIMEOUT);
	}

	public ODatabasePoolAbstract(final int iMinSize, final int iMaxSize, final boolean iEnableSecurity, final int iTimeout) {
		maxSize = iMaxSize;
		enableSecurity = iEnableSecurity;
		timeout = iTimeout;
	}

	public DB acquire(final String iURL, final String iUserName, final String iUserPassword) throws OLockException {
		OResourcePool<String, DB> pool = pools.get(iURL);
		if (pool == null) {
			synchronized (pools) {
				if (pool == null) {
					pool = new OResourcePool<String, DB>(maxSize, this);
					pools.put(iURL, pool);
				}
			}
		}

		return pool.getResource(iURL, timeout);
	}

	public void release(final DB iDatabase) {
		if (iDatabase instanceof ODatabaseRecord<?>)
			// ASSURE TO ROOL BACK ALL PENDING OPERATIONS
			((ODatabaseRecord<?>) iDatabase).rollback();

		final OResourcePool<String, DB> pool = pools.get(iDatabase.getURL());
		if (pool == null)
			throw new OLockException("Can't release a database URL not acquired before. URL: " + iDatabase.getURL());

		pool.returnResource(iDatabase);
	}

	public DB reuseResource(final String iKey, final DB iValue) {
		return iValue;
	}

	public Map<String, OResourcePool<String, DB>> getPools() {
		return pools;
	}
}
