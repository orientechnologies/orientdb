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

import java.io.IOException;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;

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

	@Override
	public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		// if (!manager.isDistributedConfiguration())
		// return;

		try {
			switch (iType) {
			case BEFORE_CREATE:
			case BEFORE_UPDATE:
			case BEFORE_DELETE:
				// CHECK IF THE CURRENT NODE IS THE OWNER FOR THAT CLUSTER
				final String clusterName = iRecord.getDatabase().getClusterNameById(iRecord.getIdentity().getClusterId());

				if (!manager.isCurrentNodeTheClusterOwner(iRecord.getDatabase().getName(), clusterName)) {
					final ODocument servers = manager.getServersForCluster(iRecord.getDatabase().getName(), clusterName);
					throw new ODistributedException("Can't apply changes to the cluster '" + clusterName
							+ "' because the current node is not the owner for that record cluster. Please connect to the server: "
							+ servers.field("owner"));
				}
				break;

			case AFTER_CREATE:
				manager.distributeRequest(new OTransactionRecordEntry((ORecordInternal<?>) iRecord, OTransactionRecordEntry.CREATED, null));
				break;

			case AFTER_UPDATE:
				manager.distributeRequest(new OTransactionRecordEntry((ORecordInternal<?>) iRecord, OTransactionRecordEntry.UPDATED, null));
				break;

			case AFTER_DELETE:
				manager.distributeRequest(new OTransactionRecordEntry((ORecordInternal<?>) iRecord, OTransactionRecordEntry.DELETED, null));
				break;
			}
		} catch (IOException e) {
			throw new ODistributedSynchronizationException("Error on distribution of the record to the configured cluster", e);
		}
		return false;
	}

	/**
	 * Install the itself as trigger to catch all the events against records
	 */
	@Override
	public void onOpen(final ODatabase iDatabase) {
		((ODatabaseComplex<?>) iDatabase).registerHook(this);
	}

	/**
	 * Remove itself as trigger to catch all the events against records
	 */
	@Override
	public void onClose(final ODatabase iDatabase) {
		((ODatabaseComplex<?>) iDatabase).unregisterHook(this);
	}
}
