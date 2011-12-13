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
package com.orientechnologies.orient.server.replication;

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
import com.orientechnologies.orient.server.network.protocol.distributed.ODistributedRequesterThreadLocal;

/**
 * Record hook implementation. Catches all the relevant events and propagates to the other cluster nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OReplicatorRecordHook implements ORecordHook, ODatabaseLifecycleListener {

	private OReplicator	replicator;

	/**
	 * Auto install itself as lifecycle listener for databases.
	 */
	public OReplicatorRecordHook(final OReplicator iReplicator) {
		replicator = iReplicator;
		Orient.instance().addDbLifecycleListener(this);
	}

	@Override
	public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		if (ODistributedRequesterThreadLocal.INSTANCE.get())
			// REPLICATED RECORD, AVOID TO PROPAGATE IT AGAIN
			return false;

		if (iRecord instanceof ODocument && replicator.isIgnoredDocumentClass(((ODocument) iRecord).getClassName()))
			// AVOID TO REPLICATE THE CONFLICT
			return false;

		try {
			switch (iType) {
			case AFTER_CREATE:
				replicator.distributeRequest(new OTransactionRecordEntry((ORecordInternal<?>) iRecord, OTransactionRecordEntry.CREATED,
						null));
				break;

			case AFTER_UPDATE:
				replicator.distributeRequest(new OTransactionRecordEntry((ORecordInternal<?>) iRecord, OTransactionRecordEntry.UPDATED,
						null));
				break;

			case AFTER_DELETE:
				replicator.distributeRequest(new OTransactionRecordEntry((ORecordInternal<?>) iRecord, OTransactionRecordEntry.DELETED,
						null));
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
