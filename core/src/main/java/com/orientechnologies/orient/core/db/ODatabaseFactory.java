/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.util.HashSet;
import java.util.WeakHashMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;

/**
 * Factory to create high-level ODatabase instances. The global instance is managed by Orient class.
 * 
 * @author Luca Garulli
 * 
 */
public class ODatabaseFactory {
	final WeakHashMap<ODatabaseComplex<?>, Thread>	instances	= new WeakHashMap<ODatabaseComplex<?>, Thread>();

	public synchronized ODatabaseComplex<?> register(final ODatabaseComplex<?> db) {
		instances.put(db, Thread.currentThread());
		return db;
	}

	public synchronized void unregister(final ODatabaseComplex<?> db) {
		instances.remove(db);
	}

	/**
	 * Closes all open databases.
	 */
	public synchronized void shutdown() {
		if (instances.size() > 0) {
			OLogManager.instance().debug(null,
					"Found %d databases opened during OrientDB shutdown. Assure to always close database instances after usage",
					instances.size());

			for (ODatabaseComplex<?> db : new HashSet<ODatabaseComplex<?>>(instances.keySet())) {
				if (db != null && !db.isClosed()) {
					db.close();
				}
			}
		}
	}

	public ODatabaseDocumentTx createDatabase(final String iType, final String url) {
		if ("graph".equals(iType))
			return new OGraphDatabase(url);
		else
			return new ODatabaseDocumentTx(url);
	}

	public ODatabaseDocumentTx createObjectDatabase(final String url) {
		return new ODatabaseDocumentTx(url);
	}

	public OGraphDatabase createGraphDatabase(final String url) {
		return new OGraphDatabase(url);
	}

	public ODatabaseDocumentTx createDocumentDatabase(final String url) {
		return new ODatabaseDocumentTx(url);
	}
}
