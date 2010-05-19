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
package com.orientechnologies.orient.kv;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.server.OServerMain;

public class OSharedBinaryDatabase {
	// TODO: ALLOW ONLY 1 BECAUSE THE TREE IS NOT YET FULLY TRANSACTIONAL
	private static final ODatabasePool<ODatabaseBinary>	dbPool	= new ODatabasePool<ODatabaseBinary>(1) {

																																public ODatabaseBinary createNewResource(final String iDatabaseName) {
																																	final String[] parts = iDatabaseName.split(":");

																																	final String path = OServerMain.server().getStoragePath(parts[0]);

																																	final ODatabaseBinary db = new ODatabaseBinary(path);

																																	if (path.startsWith(OEngineMemory.NAME)) {
																																		// CREATE AND PUT IN THE MEMORY MAPTABLE TO AVOID LOCKING (IT'S
																																		// THREAD SAFE)
																																		db.create();
																																		OServerMain.server().getMemoryDatabases()
																																				.put(iDatabaseName, db);
																																	} else
																																		db.open(parts[1], parts[2]);

																																	// DISABLE CACHE SINCE THERE IS HAZELCAST FOR IT
																																	((ODatabaseRaw) db.getUnderlying()).setUseCache(false);

																																	return db;
																																}
																															};

	public static ODatabaseBinary acquireDatabase(String iName) throws InterruptedException {
		return dbPool.acquireDatabase(iName);
	}

	public static void releaseDatabase(final ODatabaseBinary iDatabase) {
		dbPool.releaseDatabase(iDatabase.getName() + ":" + iDatabase.getUser().getName(), iDatabase);
	}
}
