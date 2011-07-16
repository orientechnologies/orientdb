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
package com.orientechnologies.orient.core.index;

import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;

/**
 * Abstract unique index class.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexUnique extends OIndexMVRBTreeAbstract {
	public OIndexUnique() {
		super("UNIQUE");
	}

	public OIndex put(final Object iKey, final OIdentifiable iSingleValue) {
		checkForOptimization();
		acquireExclusiveLock();
		try {
			checkForKeyType(iKey);

			Set<OIdentifiable> values = map.get(iKey);
			checkForOptimization();

			if (values == null)
				values = new ORecordLazySet(configuration.getDatabase()).setRidOnly(true).setRidOnly(true);
			else if (values.size() == 1) {
				// CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
				if (!values.contains(iSingleValue))
					throw new OIndexException("Found duplicated key '" + iKey + "' on unique index '" + name + "' for record "
							+ iSingleValue.getIdentity() + ". The record already present in the index is "
							+ values.iterator().next().getIdentity());
				else
					return this;
			} else if (values.size() > 1)
				throw new OIndexException("Found duplicated key '" + iKey + "' on unique index '" + name + "' for record "
						+ iSingleValue.getIdentity() + ". The record already present in the index are " + values);

			values.add(iSingleValue);

			map.put(iKey, values);
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
		// CHECK IF ALREADY EXIST
		Set<OIdentifiable> indexedRIDs = get(iKey);
		if (indexedRIDs != null && !indexedRIDs.isEmpty()) {
			if (!indexedRIDs.contains(iRecord))
				OLogManager.instance().exception("Found duplicated key '%s' previously assigned to the record %s", null,
						OIndexException.class, iKey, indexedRIDs.iterator().next());
		}
	}
}
