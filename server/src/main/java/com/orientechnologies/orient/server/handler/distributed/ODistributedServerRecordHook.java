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
package com.orientechnologies.orient.server.handler.distributed;

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.server.OClientConnection;

/**
 * Record hook implementation. Catches all the relevant events and propagates to the cluster's slave nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedServerRecordHook implements ORecordHook {

	private ODistributedServerManager	manager;
	private OClientConnection					connection;

	public ODistributedServerRecordHook(final ODistributedServerManager iDistributedServerManager, final OClientConnection iConnection) {
		manager = iDistributedServerManager;
		connection = iConnection;
	}

	public void onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		if (!manager.isDistributedConfiguration())
			return;

		switch (iType) {
		case AFTER_CREATE:
			manager.distributeRequest(connection, new OTransactionEntry<ORecordInternal<?>>((ORecordInternal<?>) iRecord,
					OTransactionEntry.CREATED, null));
			break;

		case AFTER_UPDATE:
			manager.distributeRequest(connection, new OTransactionEntry<ORecordInternal<?>>((ORecordInternal<?>) iRecord,
					OTransactionEntry.UPDATED, null));
			break;

		case AFTER_DELETE:
			manager.distributeRequest(connection, new OTransactionEntry<ORecordInternal<?>>((ORecordInternal<?>) iRecord,
					OTransactionEntry.DELETED, null));
			break;

		default:
			// NOT DISTRIBUTED REQUEST, JUST RETURN
			return;
		}

		System.out.println("\nCatched update to database: " + iType + " record: " + iRecord);
	}
}
