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

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexUnique extends OIndexOneValue {
	public OIndexUnique() {
		super("UNIQUE");
	}

	public OIndexOneValue put(final Object iKey, final OIdentifiable iSingleValue) {
		acquireExclusiveLock();
		try {
			checkForKeyType(iKey);

			OIdentifiable value = map.get(iKey);

			if (value != null) {
				// CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
				if (!value.equals(iSingleValue))
					throw new OIndexException("Found duplicated key '" + iKey + "' on unique index '" + name + "' for record "
							+ iSingleValue.getIdentity() + ". The record already present in the index is " + value.getIdentity());
				else
					return this;
			}

			map.put(iKey, iSingleValue);
			return this;

		} finally {
			releaseExclusiveLock();
		}
	}
}
