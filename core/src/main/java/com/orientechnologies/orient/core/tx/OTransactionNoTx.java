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
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;

public class OTransactionNoTx extends OTransactionAbstract {
	public OTransactionNoTx(final ODatabaseRecordTx iDatabase) {
		super(iDatabase);
		status = TXSTATUS.BEGUN;
	}

	public void begin() {
	}

	public void commit() {
		// invokeCommitAgainstListeners();
	}

	public void rollback() {
		// invokeRollbackAgainstListeners();
	}

	public ORecordInternal<?> load(final ORID iRid, final ORecordInternal<?> iRecord, final String iFetchPlan) {
		if (iRid.isNew())
			return null;

		return database.executeReadRecord((ORecordId) iRid, iRecord, iFetchPlan, false);
	}

	/**
	 * Update the record without checking the version coherence.
	 */
	public void save(final ORecordInternal<?> iContent, final String iClusterName) {
		database.executeSaveRecord(iContent, iClusterName, -1, iContent.getRecordType());
	}

	/**
	 * Delete the record without checking the version coherence.
	 */
	public void delete(final ORecordInternal<?> iRecord) {
		database.executeDeleteRecord(iRecord, -1);
	}

	public Collection<OTransactionEntry> getEntries() {
		return null;
	}

	public List<OTransactionEntry> getEntriesByClass(String iClassName) {
		return null;
	}

	public List<OTransactionEntry> getEntriesByClusterIds(int[] iIds) {
		return null;
	}

	public void clearEntries() {
	}

	public int size() {
		return 0;
	}

	public ORecordInternal<?> getEntry(ORecordId rid) {
		return null;
	}

	public boolean isUsingLog() {
		return false;
	}

	public void setUsingLog(final boolean useLog) {
	}
}
