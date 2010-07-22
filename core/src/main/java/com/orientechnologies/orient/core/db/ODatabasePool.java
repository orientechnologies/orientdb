package com.orientechnologies.orient.core.db;

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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;

public class ODatabasePool {
	private static ODatabasePoolAbstract<ODatabase>	dbPool;
	private static volatile Object									lock	= new Object();

	public static void setup() {
		setup(1, 20);
	}

	public static void setup(final int iMinSize, final int iMaxSize) {
		if (dbPool == null)
			synchronized (lock) {
				if (dbPool == null) {
					dbPool = new ODatabasePoolAbstract<ODatabase>(iMinSize, iMaxSize, true) {

						public ODatabaseDocumentTx createNewResource(final String iDatabaseName, final String... iAdditionalArgs) {
							if (iAdditionalArgs.length < 2)
								throw new OSecurityAccessException("Username and/or password missed");

							final ODatabaseDocumentTx db = new ODatabaseDocumentTx(iDatabaseName);

							db.open(iAdditionalArgs[0], iAdditionalArgs[0]);

							return db;
						}
					};
				}
			}
	}

	public static ODatabase acquire(final String iName, final String iUserName, final String iUserPassword) {
		setup();
		return dbPool.acquire(iName, iUserName, iUserPassword);
	}

	public static void release(final ODatabase iDatabase) {
		if (dbPool == null)
			throw new OConfigurationException("Database pool is not initialized");

		dbPool.release(iDatabase);
	}
}
