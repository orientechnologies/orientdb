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
package com.orientechnologies.orient.core.dictionary;

import java.util.HashMap;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

public abstract class ODictionaryAbstract<T extends Object> implements ODictionaryInternal<T>, ODatabaseListener {
	protected HashMap<String, T>	transactionalEntries;

	public ODictionaryAbstract(final ODatabaseRecord iDatabase) {
		iDatabase.registerListener(this);
	}

	public void onCreate(final ODatabase iDatabase) {
	}

	public void onDelete(final ODatabase iDatabase) {
	}

	public void onOpen(final ODatabase iDatabase) {
	}

	public void onBeforeTxBegin(final ODatabase iDatabase) {
	}

	public void onBeforeTxRollback(final ODatabase iDatabase) {
	}

	public void onAfterTxRollback(final ODatabase iDatabase) {
	}

	public void onBeforeTxCommit(final ODatabase iDatabase) {
	}

	/**
	 * Re-save all the record with a temporary ID with the definitive ones
	 */
	public void onAfterTxCommit(final ODatabase iDatabase) {
		if (transactionalEntries != null) {
			for (Entry<String, T> entry : transactionalEntries.entrySet()) {
				put(entry.getKey(), entry.getValue());
			}
			transactionalEntries.clear();
			transactionalEntries = null;
		}
	}

	public void onClose(final ODatabase iDatabase) {
	}
}
