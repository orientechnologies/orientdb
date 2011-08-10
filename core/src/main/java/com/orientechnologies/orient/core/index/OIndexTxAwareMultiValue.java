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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit(). All the other operations are
 * delegated to the wrapped OIndex instance.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexTxAwareMultiValue extends OIndexTxAware<Collection<OIdentifiable>> {
	public OIndexTxAwareMultiValue(ODatabaseRecord iDatabase, OIndex<Collection<OIdentifiable>> iDelegate) {
		super(iDatabase, iDelegate);
	}

	@Override
	public Collection<OIdentifiable> get(final Object iKey) {

		final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

		final List<OIdentifiable> result;
		if (indexChanges == null || !indexChanges.cleared)
			// BEGIN FROM THE UNDERLYING RESULT SET
			result = new ArrayList<OIdentifiable>(super.get(iKey));
		else
			// BEGIN FROM EMPTY RESULT SET
			result = new ArrayList<OIdentifiable>();

		// FILTER RESULT SET WITH TRANSACTIONAL CHANGES
		if (indexChanges != null && indexChanges.containsChangesPerKey(iKey)) {
			final OTransactionIndexChangesPerKey value = indexChanges.getChangesPerKey(iKey);
			if (value != null) {
				for (OTransactionIndexEntry entry : value.entries) {
					if (entry.operation == OPERATION.REMOVE) {
						if (entry.value == null) {
							// REMOVE THE ENTIRE KEY, SO RESULT SET IS EMPTY
							result.clear();
							break;
						} else
							// REMOVE ONLY THIS RID
							result.remove(entry.value);
					} else if (entry.operation == OPERATION.PUT) {
						// ADD ALSO THIS RID
						result.add(entry.value);
					}
				}
			}
		}

		return result;
	}
}
