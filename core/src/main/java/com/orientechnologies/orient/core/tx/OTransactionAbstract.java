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

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;

public abstract class OTransactionAbstract<REC extends ORecordInternal<?>> implements OTransaction<REC> {
	protected final ODatabaseRecordTx<REC>	database;
	protected int														id;
	protected TXSTATUS											status	= TXSTATUS.INVALID;

	protected OTransactionAbstract(ODatabaseRecordTx<REC> iDatabase, int iId) {
		database = iDatabase;
		id = iId;
	}

	public int getId() {
		return id;
	}

	public TXSTATUS getStatus() {
		return status;
	}

	protected void checkTransaction() {
		if (status == TXSTATUS.INVALID)
			throw new OTransactionException("Invalid state of the transaction. The transaction must be begun.");
	}

	public static void updateCacheFromEntries(final OStorage iStorage, final OTransaction<?> iTx,
			final Iterable<? extends OTransactionEntry<?>> iEntries) throws IOException {
		String rid;
		ORawBuffer cachedBuffer;
		for (OTransactionEntry<? extends ORecord<?>> txEntry : iEntries) {
			rid = txEntry.record.getIdentity().toString();

			cachedBuffer = iStorage.getCache().getRecord(rid);

			if (cachedBuffer != null) {
				// UPDATE CACHE
				cachedBuffer.buffer = txEntry.record.toStream();
				cachedBuffer.version = txEntry.record.getVersion();
				cachedBuffer.recordType = txEntry.record.getRecordType();

			} else if (txEntry.record.isPinned())
				// INSERT NEW ENTRY IN THE CACHE
				iStorage.getCache().pushRecord(rid,
						new ORawBuffer(txEntry.record.toStream(), txEntry.record.getVersion(), txEntry.record.getRecordType()));
		}
	}
}
