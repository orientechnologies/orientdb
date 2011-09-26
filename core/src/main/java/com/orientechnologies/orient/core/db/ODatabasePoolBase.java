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

	protected abstract void setup(int iMin, int iMax);

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
		dbPool.close();
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
