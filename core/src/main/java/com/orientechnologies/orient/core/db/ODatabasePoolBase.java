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

import java.util.Map;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;

/**
 * Database pool base class.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class ODatabasePoolBase<DB extends ODatabase> extends Thread {
	protected ODatabasePoolAbstract<DB>	dbPool;

	public void setup() {
		setup(1, 20);
	}

	protected abstract DB createResource(Object owner, String iDatabaseName, Object... iAdditionalArgs);

	public void setup(final int iMinSize, final int iMaxSize) {
		if (dbPool == null)
			synchronized (this) {
				if (dbPool == null) {
					dbPool = new ODatabasePoolAbstract<DB>(this, iMinSize, iMaxSize) {

						public DB createNewResource(final String iDatabaseName, final Object... iAdditionalArgs) {
							if (iAdditionalArgs.length < 2)
								throw new OSecurityAccessException("Username and/or password missed");

							return createResource(owner, iDatabaseName, iAdditionalArgs);
						}

						public boolean reuseResource(final String iKey, final Object[] iAdditionalArgs, final DB iValue) {
							if (((ODatabasePooled) iValue).isUnderlyingOpen()) {
								((ODatabasePooled) iValue).reuse(owner, iAdditionalArgs);
								if (iValue.getStorage().isClosed())
									// STORAGE HAS BEEN CLOSED: REOPEN IT
									iValue.getStorage().open((String) iAdditionalArgs[0], (String) iAdditionalArgs[1], null);
								else if (!((ODatabaseComplex<?>) iValue).getUser().checkPassword((String) iAdditionalArgs[1]))
									throw new OSecurityAccessException(iValue.getName(), "User or password not valid for database: '"
											+ iValue.getName() + "'");

								return true;
							}
							return false;
						}
					};
				}
			}
	}

	public DB acquire(final String iName, final String iUserName, final String iUserPassword) {
		setup();
		return dbPool.acquire(iName, iUserName, iUserPassword);
	}

	public DB acquire(final String iName, final String iUserName, final String iUserPassword,
			final Map<String, Object> iOptionalParams) {
		setup();
		return dbPool.acquire(iName, iUserName, iUserPassword, iOptionalParams);
	}

	public void release(final DB iDatabase) {
		dbPool.release(iDatabase);
	}

	public void close() {
		if (dbPool != null){
			dbPool.close();
			dbPool = null;
		}
	}

	public int getMaxSize() {
		setup();
		return dbPool.getMaxSize();
	}

	public Map<String, OResourcePool<String, DB>> getPools() {
		return dbPool.getPools();
	}

	public void remove(final String iName, final String iUser) {
		dbPool.remove(iName, iUser);
	}

	@Override
	public void run() {
		close();
	}
}
