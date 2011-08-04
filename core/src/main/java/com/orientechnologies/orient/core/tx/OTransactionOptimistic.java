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
package com.orientechnologies.orient.core.tx;

import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

public class OTransactionOptimistic extends OTransactionRealAbstract {
	private boolean								usingLog;
	private static AtomicInteger	txSerial	= new AtomicInteger();

	public OTransactionOptimistic(final ODatabaseRecordTx iDatabase) {
		super(iDatabase, txSerial.incrementAndGet());
		usingLog = OGlobalConfiguration.TX_USE_LOG.getValueAsBoolean();
	}

	public void begin() {
		status = TXSTATUS.BEGUN;
	}

	public void commit() {
		checkTransaction();
		status = TXSTATUS.COMMITTING;
		database.executeCommit();
	}

	public void rollback() {
		checkTransaction();

		status = TXSTATUS.ROLLBACKING;

		// ROLLBACK AT STORAGE LEVEL DONE IN THE COMMIT PHASE ONLY
		// database.getStorage().rollback(this);

		// INVALIDATE THE CACHE
		database.getLevel1Cache().invalidate();

		// REMOVE ALL THE ENTRIES AND INVALIDATE THE DOCUMENTS TO AVOID TO BE RE-USED DIRTY AT USER-LEVEL. IN THIS WAY RE-LOADING MUST
		// EXECUTED
		for (OTransactionRecordEntry v : recordEntries.values())
			v.getRecord().unload();

		for (OTransactionRecordEntry v : allEntries.values())
			v.getRecord().unload();

	}

	public ORecordInternal<?> loadRecord(final ORID iRid, final ORecordInternal<?> iRecord, final String iFetchPlan) {
		checkTransaction();

		final ORecordInternal<?> txRecord = getRecord(iRid);

		if (txRecord != null)
			return txRecord;

		// DELEGATE TO THE STORAGE
		return database.executeReadRecord((ORecordId) iRid, iRecord, iFetchPlan, false);
	}

	public void deleteRecord(final ORecordInternal<?> iRecord) {
		addRecord(iRecord, OTransactionRecordEntry.DELETED, null);
	}

	public void saveRecord(final ORecordInternal<?> iRecord, final String iClusterName) {
		addRecord(iRecord, iRecord.getIdentity().isValid() ? OTransactionRecordEntry.UPDATED : OTransactionRecordEntry.CREATED,
				iClusterName);
	}

	private void addRecord(final ORecordInternal<?> iRecord, final byte iStatus, final String iClusterName) {
		checkTransaction();

		if ((status == OTransaction.TXSTATUS.COMMITTING) && database.getStorage() instanceof OStorageEmbedded) {
			// I'M COMMITTING OR IT'S AN INDEX: BYPASS LOCAL BUFFER
			switch (iStatus) {
			case OTransactionRecordEntry.CREATED:
			case OTransactionRecordEntry.UPDATED:
				database.executeSaveRecord(iRecord, iClusterName, iRecord.getVersion(), iRecord.getRecordType());
				break;
			case OTransactionRecordEntry.DELETED:
				database.executeDeleteRecord(iRecord, iRecord.getVersion());
				break;
			}
		} else {
			final ORecordId rid = (ORecordId) iRecord.getIdentity();

			if (!rid.isValid()) {
				// // TODO: NEED IT FOR REAL?
				// // NEW RECORD: CHECK IF IT'S ALREADY IN
				// for (OTransactionRecordEntry entry : recordEntries.values()) {
				// if (entry.getRecord() == iRecord)
				// return;
				// }

				iRecord.onBeforeIdentityChanged(rid);

				// ASSIGN A UNIQUE SERIAL TEMPORARY ID
				rid.clusterPosition = newObjectCounter--;

				iRecord.onAfterIdentityChanged(iRecord);
			} else
				// REMOVE FROM THE DB'S CACHE
				database.getLevel1Cache().freeRecord(rid);

			OTransactionRecordEntry txEntry = getRecordEntry(rid);

			if (txEntry == null) {
				// NEW ENTRY: JUST REGISTER IT
				txEntry = new OTransactionRecordEntry(iRecord, iStatus, iClusterName);

				recordEntries.put(rid, txEntry);
			} else {
				// UPDATE PREVIOUS STATUS
				txEntry.setRecord(iRecord);

				switch (txEntry.status) {
				case OTransactionRecordEntry.LOADED:
					switch (iStatus) {
					case OTransactionRecordEntry.UPDATED:
						txEntry.status = OTransactionRecordEntry.UPDATED;
						break;
					case OTransactionRecordEntry.DELETED:
						txEntry.status = OTransactionRecordEntry.DELETED;
						break;
					}
					break;
				case OTransactionRecordEntry.UPDATED:
					switch (iStatus) {
					case OTransactionRecordEntry.DELETED:
						txEntry.status = OTransactionRecordEntry.DELETED;
						break;
					}
					break;
				case OTransactionRecordEntry.DELETED:
					break;
				case OTransactionRecordEntry.CREATED:
					switch (iStatus) {
					case OTransactionRecordEntry.DELETED:
						recordEntries.remove(rid);
						break;
					}
					break;
				}
			}
		}
	}

	@Override
	public String toString() {
		return "OTransactionOptimistic [id=" + id + ", status=" + status + ", recEntries=" + recordEntries.size() + ", idxEntries="
				+ indexEntries.size() + "]";
	}

	public boolean isUsingLog() {
		return usingLog;
	}

	public void setUsingLog(final boolean useLog) {
		this.usingLog = useLog;
	}
}
