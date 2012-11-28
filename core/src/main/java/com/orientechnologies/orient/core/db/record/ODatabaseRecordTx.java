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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.exception.OTransactionBlockedException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.ORecordCallback;
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
	public static final String	TYPE	= "record";
	private OTransaction				currentTx;

	public ODatabaseRecordTx(final String iURL, final byte iRecordType) {
		super(iURL, iRecordType);
		init();
	}

	public ODatabaseRecord begin() {
		return begin(TXTYPE.OPTIMISTIC);
	}

	public ODatabaseRecord begin(final TXTYPE iType) {
		setCurrentDatabaseinThreadLocal();

		if (currentTx.isActive())
			currentTx.rollback();

		// WAKE UP LISTENERS
		for (ODatabaseListener listener : underlying.getListeners())
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
		currentTx.rollback();

		// WAKE UP LISTENERS
		for (ODatabaseListener listener : underlying.getListeners())
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
		setCurrentDatabaseinThreadLocal();
		// WAKE UP LISTENERS
		for (ODatabaseListener listener : underlying.getListeners())
			try {
				listener.onBeforeTxCommit(this);
			} catch (Throwable t) {
				try {
					rollback();
				} catch (Exception e) {
				}
				OLogManager.instance().debug(this, "Cannot commit the transaction: caught exception on execution of %s.onBeforeTxCommit()",
						t, OTransactionBlockedException.class, listener.getClass());
			}

		try {
			currentTx.commit();
		} catch (RuntimeException e) {
			// WAKE UP ROLLBACK LISTENERS
			for (ODatabaseListener listener : underlying.getListeners())
				try {
					listener.onBeforeTxRollback(underlying);
				} catch (Throwable t) {
					OLogManager.instance().error(this, "Error before tx rollback", t);
				}
			// ROLLBACK TX AT DB LEVEL
			currentTx.rollback();
			// WAKE UP ROLLBACK LISTENERS
			for (ODatabaseListener listener : underlying.getListeners())
				try {
					listener.onAfterTxRollback(underlying);
				} catch (Throwable t) {
					OLogManager.instance().error(this, "Error after tx rollback", t);
				}
			throw e;
		}

		// WAKE UP LISTENERS
		for (ODatabaseListener listener : underlying.getListeners())
			try {
				listener.onAfterTxCommit(underlying);
			} catch (Throwable t) {
				OLogManager
						.instance()
						.debug(
								this,
								"Error after the transaction has been committed. The transaction remains valid. The exception caught was on execution of %s.onAfterTxCommit()",
								t, OTransactionBlockedException.class, listener.getClass());
			}

		return this;
	}

	public ODatabaseRecord rollback() {
		if (currentTx.isActive()) {
			// WAKE UP LISTENERS
			for (ODatabaseListener listener : underlying.getListeners())
				try {
					listener.onBeforeTxRollback(underlying);
				} catch (Throwable t) {
					OLogManager.instance().error(this, "Error before tx rollback", t);
				}

			currentTx.rollback();

			// WAKE UP LISTENERS
			for (ODatabaseListener listener : underlying.getListeners())
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
		return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord) {
		return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId) {
		return (RET) currentTx.loadRecord(iRecordId, null, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan) {
		return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan);
	}

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final OPERATION_MODE iMode,
                                                   boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
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

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecordInternal<?>> RET save(final ORecordInternal<?> iContent, final String iClusterName,
                                                   final OPERATION_MODE iMode, boolean iForceCreate, ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    currentTx.saveRecord(iContent, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
    return (RET) iContent;
  }

	@Override
	public ODatabaseRecord delete(final ORecordInternal<?> iRecord) {
		currentTx.deleteRecord(iRecord, OPERATION_MODE.SYNCHRONOUS);
		return this;
	}

	@Override
	public ODatabaseRecord delete(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
		currentTx.deleteRecord(iRecord, iMode);
		return this;
	}

	public void executeRollback(final OTransaction iTransaction) {
	}

	protected void checkTransaction() {
		if (currentTx == null || currentTx.getStatus() == TXSTATUS.INVALID)
			throw new OTransactionException("Transaction not started");
	}

	private void init() {
		currentTx = new OTransactionNoTx(this);
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
}
