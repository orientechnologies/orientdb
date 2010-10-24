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

import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;

public class OTransactionOptimistic<REC extends ORecordInternal<?>> extends OTransactionRealAbstract<REC> {
	public OTransactionOptimistic(final ODatabaseRecordTx<REC> iDatabase, final int iId) {
		super(iDatabase, iId);
	}

	public void begin() {
		status = TXSTATUS.BEGUN;
	}

	public void commit() {
		status = TXSTATUS.COMMITTING;

		// WAKE UP LISTENERS
		for (ODatabaseLifecycleListener listener : ((ODatabaseRaw) database.getUnderlying()).getListeners())
			try {
				listener.onTxCommit(database.getUnderlying());
			} catch (Throwable t) {
			}

		database.executeCommit();
		status = TXSTATUS.INVALID;
	}

	public void rollback() {
		status = TXSTATUS.ROLLBACKING;

		// WAKE UP LISTENERS
		for (ODatabaseLifecycleListener listener : ((ODatabaseRaw) database.getUnderlying()).getListeners())
			try {
				listener.onTxRollback(database.getUnderlying());
			} catch (Throwable t) {
			}

		// INVALIDATE THE CACHE
		database.getCache().removeRecords(entries.keySet());

		// REMOVE ALL THE ENTRIES AND INVALIDATE THE DOCUMENTS TO AVOID TO BE RE-USED DIRTY AT USER-LEVEL. IN THIS WAY RE-LOADING MUST
		// EXECUTED
		for (OTransactionEntry<REC> v : entries.values()) {
			v.record.unload();
		}
		entries.clear();

		newObjectCounter = -2;

		status = TXSTATUS.INVALID;
	}

	public REC load(final int iClusterId, final long iPosition, final REC iRecord, final String iFetchPlan) {
		checkTransaction();

		OTransactionEntry<REC> txEntry = getRecord(iClusterId, iPosition);

		if (txEntry != null) {
			switch (txEntry.status) {
			case OTransactionEntry.LOADED:
			case OTransactionEntry.UPDATED:
			case OTransactionEntry.CREATED:
				return txEntry.record;

			case OTransactionEntry.DELETED:
				return null;
			}
		}

		// DELEGATE TO THE STORAGE
		return database.executeReadRecord(iClusterId, iPosition, iRecord, iFetchPlan);
	}

	public void delete(final REC iRecord) {
		addRecord(iRecord, OTransactionEntry.DELETED, null);
	}

	public void save(final REC iRecord, final String iClusterName) {
		addRecord(iRecord, iRecord.getIdentity().isValid() ? OTransactionEntry.UPDATED : OTransactionEntry.CREATED, iClusterName);
	}

	private void addRecord(final REC iRecord, final byte iStatus, final String iClusterName) {
		checkTransaction();

		if (status == OTransaction.TXSTATUS.COMMITTING) {
			// I'M COMMITTING, BYPASS LOCAL BUFFER
			switch (iStatus) {
			case OTransactionEntry.CREATED:
			case OTransactionEntry.UPDATED:
				database.executeSaveRecord(iRecord, iClusterName, iRecord.getVersion(), iRecord.getRecordType());
				break;
			case OTransactionEntry.DELETED:
				database.executeDeleteRecord(iRecord, iRecord.getVersion());
				break;
			}
		} else {
			final ORecordId rid = (ORecordId) iRecord.getIdentity();

			if (!rid.isValid()) {
				// TODO: NEET IT FOR REAL?
				// NEW RECORD: CHECK IF IT'S ALREADY IN
				for (OTransactionEntry<REC> entry : entries.values()) {
					if (entry.record == iRecord)
						return;
				}

				// ASSIGN A UNIQUE SERIAL TEMPORARY ID
				rid.clusterPosition = newObjectCounter--;
			}

			OTransactionEntry<REC> txEntry = entries.get(rid);

			if (txEntry == null) {
				// NEW ENTRY: JUST REGISTER IT
				txEntry = new OTransactionEntry<REC>(iRecord, iStatus, iClusterName);

				entries.put(rid, txEntry);
			} else {
				// UPDATE PREVIOUS STATUS
				txEntry.record = iRecord;

				switch (txEntry.status) {
				case OTransactionEntry.LOADED:
					switch (iStatus) {
					case OTransactionEntry.UPDATED:
						txEntry.status = OTransactionEntry.UPDATED;
						break;
					case OTransactionEntry.DELETED:
						txEntry.status = OTransactionEntry.DELETED;
						break;
					}
					break;
				case OTransactionEntry.UPDATED:
					switch (iStatus) {
					case OTransactionEntry.DELETED:
						txEntry.status = OTransactionEntry.DELETED;
						break;
					}
					break;
				case OTransactionEntry.DELETED:
					break;
				case OTransactionEntry.CREATED:
					switch (iStatus) {
					case OTransactionEntry.DELETED:
						entries.remove(rid);
						break;
					}
					break;
				}
			}
		}
	}

	private OTransactionEntry<REC> getRecord(final int iClusterId, final long iPosition) {
		return entries.get(new ORecordId(iClusterId, iPosition));
	}

	@Override
	public String toString() {
		return "OTransactionOptimistic [id=" + id + ", status=" + status + ", entries=" + entries.size() + "]";
	}
}
