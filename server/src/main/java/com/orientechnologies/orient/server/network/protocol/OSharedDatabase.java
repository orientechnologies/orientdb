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
package com.orientechnologies.orient.server.network.protocol;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.server.OServerMain;

public class OSharedDatabase {
	// TODO: ALLOW ONLY 1 BECAUSE THE TREE IS NOT YET FULLY TRANSACTIONAL
	private static final ODatabasePool<ODatabaseBinary>	dbPool	= new ODatabasePool<ODatabaseBinary>(1) {

																																public ODatabaseBinary createNewResource(String iDatabaseName) {
																																	ODatabaseBinary db = new ODatabaseBinary("local:"
																																			+ OServerMain.server().getStoragePath(iDatabaseName)).open(
																																			"admin", "admin");

																																	// DISABLE CACHE SINCE THERE IS HAZELCAST FOR IT
																																	((ODatabaseRaw) db.getUnderlying()).setUseCache(false);

																																	return db;
																																}
																															};

	public static ODatabaseBinary acquireDatabase(String iName) throws OLockException, InterruptedException {
		return dbPool.acquireDatabase(iName);
	}

	public static void releaseDatabase(final String iName, final ODatabaseBinary iDatabase) throws OLockException,
			InterruptedException {
		dbPool.releaseDatabase(iName, iDatabase);
	}
}
