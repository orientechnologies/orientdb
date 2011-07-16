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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Abstract not unique index class.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexNotUnique extends OIndexMVRBTreeAbstract {
	public OIndexNotUnique() {
		super("NOTUNIQUE");
	}

	public OIndex put(final Object iKey, final OIdentifiable iSingleValue) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			checkForKeyType(iKey);

			Set<OIdentifiable> values = map.get(iKey);
			checkForOptimization();
			if (values == null)
				values = new ORecordLazySet(configuration.getDatabase()).setRidOnly(true);

			if (!iSingleValue.getIdentity().isValid())
				((ORecord<?>) iSingleValue).save();

			values.add(iSingleValue);

			map.put(iKey, values);
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}

	public boolean remove(final Object iKey, final OIdentifiable iValue) {
		checkForOptimization();
		acquireExclusiveLock();
		try {

			final Set<OIdentifiable> recs = get(iKey);
			if (recs != null && !recs.isEmpty()) {
				if (recs.remove(iValue)) {
					map.put(iKey, recs);
					return true;
				}
			}
			return false;

		} finally {
			releaseExclusiveLock();
		}
	}
}
