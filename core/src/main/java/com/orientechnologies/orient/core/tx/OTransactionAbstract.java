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
import com.orientechnologies.orient.core.cache.OLevel1RecordCache;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.storage.OStorage;

public abstract class OTransactionAbstract implements OTransaction {
	protected final ODatabaseRecordTx	database;
	protected TXSTATUS								status	= TXSTATUS.INVALID;

	protected OTransactionAbstract(ODatabaseRecordTx iDatabase) {
		database = iDatabase;
	}

	public boolean isActive() {
		return status != TXSTATUS.INVALID;
	}

	public TXSTATUS getStatus() {
		return status;
	}

	public ODatabaseRecordTx getDatabase() {
		return database;
	}

	public static void updateCacheFromEntries(final OStorage iStorage, final OTransaction iTx,
			final Iterable<? extends OTransactionRecordEntry> iEntries) throws IOException {
		final OLevel1RecordCache dbCache = (OLevel1RecordCache) iTx.getDatabase().getLevel1Cache();

		for (OTransactionRecordEntry txEntry : iEntries) {
			if (txEntry.status == OTransactionRecordEntry.DELETED)
				dbCache.deleteRecord(txEntry.getRecord().getIdentity());
			else if (txEntry.status == OTransactionRecordEntry.UPDATED || txEntry.status == OTransactionRecordEntry.CREATED)
				dbCache.updateRecord(txEntry.getRecord());
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
