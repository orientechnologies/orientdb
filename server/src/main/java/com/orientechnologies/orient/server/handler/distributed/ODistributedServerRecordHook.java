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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransactionEntry;

/**
 * Record hook implementation. Catches all the relevant events and propagates to the cluster's slave nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedServerRecordHook implements ORecordHook, ODatabaseLifecycleListener {

	private ODistributedServerManager	manager;

	/**
	 * Auto install itself as lifecycle listener for databases.
	 */
	public ODistributedServerRecordHook(final ODistributedServerManager iDistributedServerManager) {
		manager = iDistributedServerManager;
		Orient.instance().addDbLifecycleListener(this);
	}

	public void onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		if (!manager.isDistributedConfiguration())
			return;

		OLogManager.instance().info(
				this,
				"Caught change " + iType + " in database '" + iRecord.getDatabase().getName() + "', record: " + iRecord.getIdentity()
						+ ". Distribute the change in all the cluster nodes");

		switch (iType) {
		case AFTER_CREATE:
			manager.distributeRequest(new OTransactionEntry<ORecordInternal<?>>((ORecordInternal<?>) iRecord, OTransactionEntry.CREATED,
					null));
			break;

		case AFTER_UPDATE:
			manager.distributeRequest(new OTransactionEntry<ORecordInternal<?>>((ORecordInternal<?>) iRecord, OTransactionEntry.UPDATED,
					null));
			break;

		case AFTER_DELETE:
			manager.distributeRequest(new OTransactionEntry<ORecordInternal<?>>((ORecordInternal<?>) iRecord, OTransactionEntry.DELETED,
					null));
			break;

		default:
			// NOT DISTRIBUTED REQUEST, JUST RETURN
			return;
		}
	}

	/**
	 * Install the itself as trigger to catch all the events against records
	 */
	public void onOpen(final ODatabase iDatabase) {
		((ODatabaseComplex<?>) iDatabase).registerHook(this);
	}

	/**
	 * Remove itself as trigger to catch all the events against records
	 */
	public void onClose(final ODatabase iDatabase) {
		((ODatabaseComplex<?>) iDatabase).unregisterHook(this);
	}
}
