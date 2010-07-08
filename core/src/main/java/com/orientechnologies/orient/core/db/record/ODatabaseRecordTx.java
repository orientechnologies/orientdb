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

import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXSTATUS;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

/**
 * Delegates all the CRUD operations to the current transaction.
 * 
 */
public class ODatabaseRecordTx<REC extends ORecordInternal<?>> extends ODatabaseRecordAbstract<REC> {
	private OTransaction<REC>		currentTx;
	private static volatile int	txSerial	= 0;

	public ODatabaseRecordTx(final String iURL, final Class<? extends REC> iRecordClass) {
		super(iURL, iRecordClass);
		init();
	}

	public ODatabaseRecord<REC> begin() {
		begin(TXTYPE.OPTIMISTIC);
		return this;
	}

	public ODatabaseRecord<REC> begin(final TXTYPE iType) {
		currentTx.rollback();

		switch (iType) {
		case NOTX:
			setDefaultTransactionMode();
			break;

		case OPTIMISTIC:
			currentTx = new OTransactionOptimistic<REC>(this, txSerial++);
			break;

		case PESSIMISTIC:
			throw new UnsupportedOperationException("Pessimistic transaction");
		}

		currentTx.begin();
		return this;
	}

	public ODatabaseRecord<REC> commit() {
		currentTx.commit();
		setDefaultTransactionMode();
		return this;
	}

	public ODatabaseRecord<REC> rollback() {
		currentTx.rollback();
		setDefaultTransactionMode();
		return this;
	}

	@Override
	public REC load(final int iClusterId, final long iPosition, final REC iRecord) {
		return currentTx.load(iClusterId, iPosition, iRecord);
	}

	@Override
	public ODatabaseRecord<REC> save(final REC iContent) {
		return save(iContent, null);
	}

	@Override
	public ODatabaseRecord<REC> save(final REC iContent, final String iClusterName) {
		currentTx.save(iContent, iClusterName);
		return this;
	}

	@Override
	public ODatabaseRecord<REC> delete(final REC iRecord) {
		currentTx.delete(iRecord);
		return this;
	}

	public void executeRollback(final OTransaction<?> iTransaction) {
	}

	public void executeCommit() {
		getStorage().commit(getId(), currentTx);
	}

	protected void checkTransaction() {
		if (currentTx == null || currentTx.getStatus() == TXSTATUS.INVALID)
			throw new OTransactionException("Transaction not started");
	}

	private void init() {
		currentTx = new OTransactionNoTx<REC>(this, -1);
	}

	public ORecordInternal<?> getRecordByUserObject(final Object iUserObject, final boolean iIsMandatory) {
		return (ORecordInternal<?>) iUserObject;
	}

	public Object getUserObjectByRecord(final ORecordInternal<?> record, final String iFetchPlan) {
		return record;
	}

	public boolean existsUserObjectByRecord(final ORecordInternal<?> iRecord) {
		return true;
	}

	private void setDefaultTransactionMode() {
		if (!(currentTx instanceof OTransactionNoTx))
			currentTx = new OTransactionNoTx<REC>(this, txSerial++);
	}
}
