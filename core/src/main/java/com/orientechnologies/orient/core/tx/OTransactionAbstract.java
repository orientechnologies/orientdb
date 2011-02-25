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

import java.io.IOException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OStorage;

public abstract class OTransactionAbstract implements OTransaction {
	protected final ODatabaseRecordTx	database;
	protected int											id;
	protected TXSTATUS								status	= TXSTATUS.INVALID;

	protected OTransactionAbstract(ODatabaseRecordTx iDatabase, int iId) {
		database = iDatabase;
		id = iId;
	}

	public int getId() {
		return id;
	}

	public TXSTATUS getStatus() {
		return status;
	}

	public ODatabaseRecordTx getDatabase() {
		return database;
	}

	protected void checkTransaction() {
		if (status == TXSTATUS.INVALID)
			throw new OTransactionException("Invalid state of the transaction. The transaction must be begun.");
	}

	public static void updateCacheFromEntries(final OStorage iStorage, final OTransaction iTx,
			final Iterable<? extends OTransactionEntry> iEntries) throws IOException {
		ORID rid;
		ODatabaseRecord db = iTx.getDatabase();
		for (OTransactionEntry txEntry : iEntries) {
			rid = txEntry.getRecord().getIdentity();

			if (txEntry.status == OTransactionEntry.DELETED)
				db.getCache().deleteRecord(rid);
			else if (txEntry.status == OTransactionEntry.UPDATED) {
				db.getCache().freeRecord(rid);
			}
		}
	}

	protected void invokeCommitAgainstListeners() {
		// WAKE UP LISTENERS
		for (ODatabaseListener listener : ((ODatabaseRaw) database.getUnderlying()).getListeners())
			try {
				listener.onBeforeTxCommit(database.getUnderlying());
			} catch (Throwable t) {
				OLogManager.instance().error(this, "Error on commit callback against listener: " + listener, t);
			}
	}

	protected void invokeRollbackAgainstListeners() {
		// WAKE UP LISTENERS
		for (ODatabaseListener listener : ((ODatabaseRaw) database.getUnderlying()).getListeners())
			try {
				listener.onBeforeTxRollback(database.getUnderlying());
			} catch (Throwable t) {
				OLogManager.instance().error(this, "Error on rollback callback against listener: " + listener, t);
			}
	}
}
