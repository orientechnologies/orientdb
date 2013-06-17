/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import com.orientechnologies.orient.core.db.record.ORecordOperation;

public abstract class OTransactionAbstract implements OTransaction {
  protected final ODatabaseRecordTx database;
  protected TXSTATUS                status = TXSTATUS.INVALID;

  protected OTransactionAbstract(final ODatabaseRecordTx iDatabase) {
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

  public static void updateCacheFromEntries(final OTransaction tx, final Iterable<? extends ORecordOperation> entries,
      final boolean updateStrategy) throws IOException {
    final OLevel1RecordCache dbCache = tx.getDatabase().getLevel1Cache();

    for (ORecordOperation txEntry : entries) {
      if (!updateStrategy)
        // ALWAYS REMOVE THE RECORD FROM CACHE
        dbCache.deleteRecord(txEntry.getRecord().getIdentity());
      else if (txEntry.type == ORecordOperation.DELETED)
        // DELETION
        dbCache.deleteRecord(txEntry.getRecord().getIdentity());
      else if (txEntry.type == ORecordOperation.UPDATED || txEntry.type == ORecordOperation.CREATED)
        // UDPATE OR CREATE
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
