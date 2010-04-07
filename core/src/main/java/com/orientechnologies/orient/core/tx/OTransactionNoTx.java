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

import java.util.Collection;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.record.ORecordInternal;

public class OTransactionNoTx<REC extends ORecordInternal<?>> extends OTransactionAbstract<REC> {
	public OTransactionNoTx(final ODatabaseRecordTx<REC> iDatabase, final int iId) {
		super(iDatabase, iId);
		status = TXSTATUS.BEGUN;
	}

	public void begin() {
	}

	public void commit() {
	}

	public void rollback() {
	}

	public REC load(final int iClusterId, final long iPosition, final REC iRecord) {
		return database.executeReadRecord(iClusterId, iPosition, iRecord);
	}

	/**
	 * Update the record without checking the version coherence.
	 */
	public void save(final REC iContent, final String iClusterName) {
		database.executeSaveRecord(iContent, iClusterName, -1, iContent.getRecordType());
	}

	/**
	 * Delete the record without checking the version coherence.
	 */
	public void delete(final REC iRecord) {
		database.executeDeleteRecord(iRecord, -1);
	}

	public Collection<OTransactionEntry<REC>> getEntries() {
		return null;
	}

	public int size() {
		return 0;
	}
}
