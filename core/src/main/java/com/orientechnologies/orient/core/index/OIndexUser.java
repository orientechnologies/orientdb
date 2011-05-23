/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexEntry.STATUSES;

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit(). All the other operations are
 * delegated to the wrapped OIndex instance.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexUser extends OIndexAbstractDelegate {
	private final OTransaction	tx;

	public OIndexUser(final OTransaction iTransaction) {
		super(null);
		tx = iTransaction;
	}

	@Override
	public OIndex put(final Object iKey, final OIdentifiable iValue) {
		tx.addIndexEntry(delegate, super.getName(), STATUSES.PUT, iKey, iValue);
		return this;
	}

	@Override
	public boolean remove(final Object iKey) {
		tx.addIndexEntry(delegate, super.getName(), STATUSES.REMOVE, iKey, null);
		return true;
	}

	@Override
	public boolean remove(final Object iKey, final OIdentifiable iRID) {
		tx.addIndexEntry(delegate, super.getName(), STATUSES.REMOVE, iKey, iRID);
		return true;
	}

	@Override
	public int remove(final OIdentifiable iRID) {
		tx.addIndexEntry(delegate, super.getName(), STATUSES.REMOVE, null, iRID);
		return 1;
	}

	@Override
	public OIndex clear() {
		tx.addIndexEntry(delegate, super.getName(), STATUSES.CLEAR, null, null);
		return this;
	}

	@Override
	public void unload() {
		tx.clearIndexEntries();
		super.unload();
	}
}
