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
package com.orientechnologies.orient.core.db.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexMVRBTreeAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

/**
 * Delegates all the CRUD operations to the current transaction.
 * 
 */
public class ODatabaseRecordTx extends ODatabaseRecordAbstract {
	private OTransaction	currentTx;

	public ODatabaseRecordTx(final String iURL, final byte iRecordType) {
		super(iURL, iRecordType);
		init();
	}

	public ODatabaseRecord begin() {
		return begin(TXTYPE.OPTIMISTIC);
	}

	public ODatabaseRecord begin(final TXTYPE iType) {
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
				OLogManager.instance().error(this, "Error before the transaction begin", t, OTransactionException.class);
			}

		currentTx = iTx;
		currentTx.begin();

		return this;
	}

	public ODatabaseRecord commit() {
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
						t, OTransactionException.class, listener.getClass());
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
								t, OTransactionException.class, listener.getClass());
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

	@Override
	public ODatabaseRecord save(final ORecordInternal<?> iContent) {
		return save(iContent, null);
	}

	@Override
	public ODatabaseRecord save(final ORecordInternal<?> iContent, final String iClusterName) {
		currentTx.saveRecord(iContent, iClusterName);
		return this;
	}

	@Override
	public ODatabaseRecord delete(final ORecordInternal<?> iRecord) {
		currentTx.deleteRecord(iRecord);
		return this;
	}

	public void executeRollback(final OTransaction iTransaction) {
	}

	@SuppressWarnings("rawtypes")
	public void executeCommit() {
		if (!(getStorage() instanceof OStorageEmbedded))
			getStorage().commit(currentTx);
		else {
			final List<String> involvedIndexes = currentTx.getInvolvedIndexes();

			// LOCK INVOLVED INDEXES
			List<OIndexMVRBTreeAbstract<?>> lockedIndexes = null;
			try {
				if (involvedIndexes != null)
					for (String indexName : involvedIndexes) {
						final OIndexMVRBTreeAbstract<?> index = (OIndexMVRBTreeAbstract<?>) getMetadata().getIndexManager().getIndexInternal(
								indexName);
						if (lockedIndexes == null)
							lockedIndexes = new ArrayList<OIndexMVRBTreeAbstract<?>>();

						index.acquireExclusiveLock();
						lockedIndexes.add(index);
					}

				getStorage().callInLock(new Callable<Void>() {

					public Void call() throws Exception {
						getStorage().commit(currentTx);

						// COMMIT INDEX CHANGES
						final ODocument indexEntries = currentTx.getIndexChanges();
						if (indexEntries != null) {
							for (Entry<String, Object> indexEntry : indexEntries) {
								final OIndex<?> index = getMetadata().getIndexManager().getIndexInternal(indexEntry.getKey());
								index.commit((ODocument) indexEntry.getValue());
							}
						}
						return null;
					}

				}, true);

			} finally {
				// RELEASE INDEX LOCKS IF ANY
				if (lockedIndexes != null)
					// DON'T USE GENERICS TO AVOID OpenJDK CRASH :-(
					for (OIndexMVRBTreeAbstract index : lockedIndexes) {
						index.releaseExclusiveLock();
					}
			}
		}
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

	public Object getUserObjectByRecord(final ORecordInternal<?> record, final String iFetchPlan) {
		return record;
	}

	public boolean existsUserObjectByRID(final ORID iRID) {
		return true;
	}

	public void setDefaultTransactionMode() {
		if (!(currentTx instanceof OTransactionNoTx))
			currentTx = new OTransactionNoTx(this);
	}
}
