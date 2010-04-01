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
import com.orientechnologies.orient.core.db.record.ODatabaseRecordBinary;
import com.orientechnologies.orient.server.OServerMain;

public abstract class ONetworkProtocolDatabaseShared extends ONetworkProtocol {
	// TODO: ALLOW ONLY 1 BECAUSE THE TREE IS NOT YET FULLY TRANSACTIONAL
	private static final ODatabasePool<ODatabaseRecordBinary>	dbPool	= new ODatabasePool<ODatabaseRecordBinary>(1) {

																																			public ODatabaseRecordBinary createNewResource(
																																					String iDatabaseName) {
																																				return new ODatabaseRecordBinary("local:"
																																						+ OServerMain.server().getStoragePath(iDatabaseName))
																																						.open("admin", "admin");
																																			}
																																		};

	protected ONetworkProtocolDatabaseShared(ThreadGroup group, String name) {
		super(group, name);
	}

	public ODatabaseRecordBinary acquireDatabase(String iName) throws OLockException, InterruptedException {
		return dbPool.acquireDatabase(iName);
	}

	public void releaseDatabase(final String iName, final ODatabaseRecordBinary iDatabase) throws OLockException,
			InterruptedException {
		dbPool.releaseDatabase(iName, iDatabase);
	}
}
