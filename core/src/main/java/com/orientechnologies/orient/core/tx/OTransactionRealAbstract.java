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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

public abstract class OTransactionRealAbstract extends OTransactionAbstract {
	protected Map<ORID, OTransactionEntry>	entries						= new HashMap<ORID, OTransactionEntry>();
	protected int														newObjectCounter	= -2;

	protected OTransactionRealAbstract(ODatabaseRecordTx iDatabase, int iId) {
		super(iDatabase, iId);
	}

	public Collection<OTransactionEntry> getEntries() {
		return entries.values();
	}

	public ORecordInternal<?> getEntry(final ORecordId rid) {
		OTransactionEntry e = entries.get(rid);
		if (e != null)
			return e.getRecord();
		return null;
	}

	public List<OTransactionEntry> getEntriesByClass(final String iClassName) {
		final List<OTransactionEntry> result = new ArrayList<OTransactionEntry>();

		if (iClassName == null || iClassName.length() == 0)
			// RETURN ALL THE RECORDS
			for (OTransactionEntry entry : entries.values()) {
				result.add(entry);
			}
		else
			// FILTER RECORDS BY CLASSNAME
			for (OTransactionEntry entry : entries.values()) {
				if (entry.getRecord() != null && entry.getRecord() instanceof ODocument
						&& iClassName.equals(((ODocument) entry.getRecord()).getClassName()))
					result.add(entry);
			}

		return result;
	}

	public List<OTransactionEntry> getEntriesByClusterIds(final int[] iIds) {
		final List<OTransactionEntry> result = new ArrayList<OTransactionEntry>();

		if (iIds == null)
			// RETURN ALL THE RECORDS
			for (OTransactionEntry entry : entries.values()) {
				result.add(entry);
			}
		else
			// FILTER RECORDS BY ID
			for (OTransactionEntry entry : entries.values()) {
				for (int id : iIds) {
					if (entry.getRecord() != null && entry.getRecord().getIdentity().getClusterId() == id) {
						result.add(entry);
						break;
					}
				}
			}

		return result;
	}

	public void clearEntries() {
		entries.clear();
	}

	public int size() {
		return entries.size();
	}

	@Override
	protected void checkTransaction() {
		if (status == TXSTATUS.INVALID)
			throw new OTransactionException("Invalid state of the transaction. The transaction must be begun.");
	}
}
