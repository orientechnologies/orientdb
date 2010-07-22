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
package com.orientechnologies.orient.server.db;

import java.util.Map;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.orient.core.db.ODatabasePoolAbstract;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.server.OServerMain;

public class OSharedDocumentDatabase {
	// TODO: ALLOW ONLY 1 BECAUSE THE TREE IS NOT YET FULLY TRANSACTIONAL
	private static final ODatabasePoolAbstract<ODatabaseDocumentTx>	dbPool	= new ODatabasePoolAbstract<ODatabaseDocumentTx>(1, 20,
																																							true) {

																																						public ODatabaseDocumentTx createNewResource(
																																								final String iDatabaseName,
																																								final String... iAdditionalArgs) {
																																							if (iAdditionalArgs.length < 2)
																																								throw new OSecurityAccessException(
																																										"Username and/or password missed");

																																							final String path = OServerMain.server()
																																									.getStoragePath(iDatabaseName);

																																							final ODatabaseDocumentTx db = new ODatabaseDocumentTx(
																																									path);

																																							if (path.startsWith(OEngineMemory.NAME)) {
																																								// CREATE AND PUT IN THE MEMORY MAPTABLE TO AVOID
																																								// LOCKING
																																								// (IT'S
																																								// THREAD SAFE)
																																								db.create();
																																								OServerMain.server().getMemoryDatabases()
																																										.put(iDatabaseName, db);
																																							} else
																																								db.open(iAdditionalArgs[0], iAdditionalArgs[1]);

																																							return db;
																																						}
																																					};

	public static ODatabaseDocumentTx acquire(final String iName, final String iUserName, final String iUserPassword)
			throws InterruptedException {
		return dbPool.acquire(iName, iUserName, iUserPassword);
	}

	public static void release(final ODatabaseDocumentTx iDatabase) {
		dbPool.release(iDatabase);
	}

	public static Map<String, OResourcePool<String, ODatabaseDocumentTx>> getDatabasePools() {
		return dbPool.getPools();
	}
}
