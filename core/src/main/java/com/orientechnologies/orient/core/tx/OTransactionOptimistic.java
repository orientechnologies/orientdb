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

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.id.ORID;
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
		database.executeCommit();
		status = TXSTATUS.INVALID;
	}

	public void rollback() {
		status = TXSTATUS.ROLLBACKING;
		entries.clear();
		status = TXSTATUS.INVALID;
	}

	public REC load(final int iClusterId, final long iPosition, final REC iRecord) {
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
		return database.executeReadRecord(iClusterId, iPosition, iRecord);
	}

	public void delete(final REC iRecord) {
		addRecord(iRecord, OTransactionEntry.DELETED, null);
	}

	public void save(final REC iContent, final String iClusterName) {
		addRecord(iContent, iContent.getIdentity().isValid() ? OTransactionEntry.UPDATED : OTransactionEntry.CREATED, iClusterName);
	}

	private void addRecord(final REC iRecord, final byte iStatus, final String iClusterName) {
		checkTransaction();

		int clusterId = iRecord.getIdentity().getClusterId();
		long position = iRecord.getIdentity().getClusterPosition();

		final StringBuilder key = new StringBuilder();
		key.append(clusterId);
		key.append(ORID.SEPARATOR);

		if (position == -1) {
			// NEW RECORD: CHECK IF IT'S ALREADY IN
			for (OTransactionEntry<REC> entry : entries.values()) {
				if (entry.record == iRecord)
					return;
			}

			// ASSIGN A UNIQUE SERIAL TEMPORARY ID
			position = newObjectCounter++;
			key.append(position);
			key.append('+');
		} else
			key.append(position);

		OTransactionEntry<REC> txEntry = entries.get(key);

		if (txEntry == null) {
			// NEW ENTRY: JUST REGISTER IT
			txEntry = new OTransactionEntry<REC>(iRecord, iStatus, iClusterName);
			entries.put(key.toString(), txEntry);
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
					entries.remove(key);
					break;
				}
				break;
			}
		}
	}

	private OTransactionEntry<REC> getRecord(final int iClusterId, final long iPosition) {
		return entries.get(ORecordId.generateString(iClusterId, iPosition));
	}
}
