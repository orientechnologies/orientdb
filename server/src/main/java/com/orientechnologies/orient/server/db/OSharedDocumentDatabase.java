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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServerMain;

public class OSharedDocumentDatabase {
	public static ODatabaseDocumentTx acquire(final String iName, final String iUserName, final String iUserPassword)
			throws InterruptedException {
		final String path = OServerMain.server().getStoragePath(iName);

		return ODatabaseDocumentPool.global().acquire(path, iUserName, iUserPassword);
	}

	public static void release(final ODatabaseDocumentTx iDatabase) {
		ODatabaseDocumentPool.global().release(iDatabase);
	}

	public static Map<String, OResourcePool<String, ODatabaseDocumentTx>> getDatabasePools() {
		return ODatabaseDocumentPool.global().getPools();
	}

	public static void remove(String iName, String iUser) {
		ODatabaseDocumentPool.global().remove(iName, iUser);
	}
}
