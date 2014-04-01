/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OTransactionBlockedException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Delegates all the CRUD operations to the current transaction.
 * 
 */
public class ODatabaseRecordTx extends ODatabaseRecordAbstract {
  public static final String TYPE = "record";
  private OTransaction       currentTx;

  public ODatabaseRecordTx(final String iURL, final byte iRecordType) {
    super(iURL, iRecordType);
    init();
  }

  public ODatabaseRecord begin() {
    return begin(TXTYPE.OPTIMISTIC);
  }

  public ODatabaseRecord begin(final TXTYPE iType) {
    setCurrentDatabaseinThreadLocal();

    if (currentTx.isActive()) {
      if (iType == TXTYPE.OPTIMISTIC && currentTx instanceof OTransactionOptimistic) {
        currentTx.begin();
        return this;
      }

      currentTx.rollback(true, 0);
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : underlying.browseListeners())
      try {
        listener.onBeforeTxBegin(underlying);
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error before tx begin", t);
      }

    switch (iType) {
    case NOTX:
      setDefaultTransactionMode();
      break;

    case OPTIMISTIC:
      currentTx = new OTransactionOptimistic(this);
      break;

    case PESSIMISTIC:
      throw new UnsupportedOperationException("Pessimistic transaction");
    }

    currentTx.begin();
    return this;
  }

  public ODatabaseRecord begin(final OTransaction iTx) {
    if (currentTx.isActive() && iTx.equals(currentTx)) {
      currentTx.begin();
      return this;
    }

    currentTx.rollback(true, 0);

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : underlying.browseListeners())
      try {
        listener.onBeforeTxBegin(underlying);
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error before the transaction begin", t, OTransactionBlockedException.class);
      }

    currentTx = iTx;
    currentTx.begin();

    return this;
  }

  public ODatabaseRecord commit() {
    return commit(false);
  }

  @Override
  public ODatabaseRecord commit(boolean force) throws OTransactionException {
    if (!currentTx.isActive())
      return this;

    if (!force && currentTx.amountOfNestedTxs() > 1) {
      currentTx.commit();
      return this;
    }

    setCurrentDatabaseinThreadLocal();
    // WAKE UP LISTENERS
    for (ODatabaseListener listener : underlying.browseListeners())
      try {
        listener.onBeforeTxCommit(this);
      } catch (Throwable t) {
        try {
          rollback(force);
        } catch (RuntimeException e) {
          throw e;
        }
        OLogManager.instance().debug(this, "Cannot commit the transaction: caught exception on execution of %s.onBeforeTxCommit()",
            t, OTransactionBlockedException.class, listener.getClass());
      }

    try {
      currentTx.commit(force);
    } catch (RuntimeException e) {
      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : underlying.browseListeners())
        try {
          listener.onBeforeTxRollback(underlying);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before tx rollback", t);
        }
      // ROLLBACK TX AT DB LEVEL
      currentTx.rollback(false, 0);
      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : underlying.browseListeners())
        try {
          listener.onAfterTxRollback(underlying);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after tx rollback", t);
        }
      throw e;
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : underlying.browseListeners())
      try {
        listener.onAfterTxCommit(underlying);
      } catch (Throwable t) {
        OLogManager
            .instance()
            .debug(this, "Error after the transaction has been committed. The transaction remains valid. The exception caught was on execution of %s.onAfterTxCommit()", t, OTransactionBlockedException.class, listener.getClass());
      }

    return this;
  }

  @Override
  public void close() {
    try {
      commit(true);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during commit of active transaction.", e);
    }

    super.close();
  }

  public ODatabaseRecord rollback() {
    return rollback(false);
  }

  @Override
  public ODatabaseRecord rollback(boolean force) throws OTransactionException {
    if (currentTx.isActive()) {

      if (!force && currentTx.amountOfNestedTxs() > 1) {
        currentTx.rollback();
        return this;
      }

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : underlying.browseListeners())
        try {
          listener.onBeforeTxRollback(underlying);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before tx rollback", t);
        }

      currentTx.rollback(force, -1);

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : underlying.browseListeners())
        try {
          listener.onAfterTxRollback(underlying);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after tx rollback", t);
        }
    }

    return this;
  }

  public OTransaction getTransaction() {
    return currentTx;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord, final String iFetchPlan) {
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET load(ORecordInternal<?> iRecord, String iFetchPlan, boolean iIgnoreCache,
      boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord) {
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, null, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId) {
    return (RET) currentTx.loadRecord(iRecordId, null, null, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan) {
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, String iFetchPlan, final boolean iIgnoreCache,
      final boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET reload(final ORecordInternal<?> iRecord) {
    return reload(iRecord, null, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET reload(final ORecordInternal<?> iRecord, final String iFetchPlan) {
    return reload(iRecord, iFetchPlan, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET reload(final ORecordInternal<?> iRecord, final String iFetchPlan,
      final boolean iIgnoreCache) {
    ORecordInternal<?> record = currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, false,
        OStorage.LOCKING_STRATEGY.DEFAULT);
    if (record != null && iRecord != record) {
      iRecord.fromStream(record.toStream());
      iRecord.getRecordVersion().copyFrom(record.getRecordVersion());
    }
    return (RET) record;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return (RET) save(iContent, (String) null, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent) {
    return (RET) save(iContent, (String) null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final String iClusterName) {
    return (RET) save(iContent, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  @Override
  public boolean updatedReplica(ORecordInternal<?> iContent) {
    return currentTx.updateReplica(iContent);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final String iClusterName,
      final OPERATION_MODE iMode, boolean iForceCreate, ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    currentTx.saveRecord(iContent, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
    return (RET) iContent;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseRecord delete(final ORID iRecord) {
    final ORecord<?> rec = iRecord.getRecord();
    if (rec != null)
      rec.delete();
    return this;
  }

  @Override
  public ODatabaseRecord delete(final ORecordInternal<?> iRecord) {
    currentTx.deleteRecord(iRecord, OPERATION_MODE.SYNCHRONOUS);
    return this;
  }

	@Override
	public boolean hide(ORID rid) {
		if (currentTx.isActive())
			throw new ODatabaseException("This operation can be executed only in non tx mode");
		return super.hide(rid);
	}

	@Override
  public ODatabaseRecord delete(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
    currentTx.deleteRecord(iRecord, iMode);
    return this;
  }

  public void executeRollback(final OTransaction iTransaction) {
  }

  public ORecordInternal<?> getRecordByUserObject(final Object iUserObject, final boolean iCreateIfNotAvailable) {
    return (ORecordInternal<?>) iUserObject;
  }

  public void registerUserObject(final Object iObject, final ORecordInternal<?> iRecord) {
  }

  public void registerUserObjectAfterLinkSave(ORecordInternal<?> iRecord) {
  }

  public Object getUserObjectByRecord(final OIdentifiable record, final String iFetchPlan) {
    return record;
  }

  public boolean existsUserObjectByRID(final ORID iRID) {
    return true;
  }

  public String getType() {
    return TYPE;
  }

  public void setDefaultTransactionMode() {
    if (!(currentTx instanceof OTransactionNoTx))
      currentTx = new OTransactionNoTx(this);
  }

  protected void checkTransaction() {
    if (currentTx == null || currentTx.getStatus() == TXSTATUS.INVALID)
      throw new OTransactionException("Transaction not started");
  }

  private void init() {
    currentTx = new OTransactionNoTx(this);
  }

}
