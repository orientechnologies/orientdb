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

import java.util.HashSet;
import java.util.Set;

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

	public OIndex put(final Object iKey, final ORecord<?> iSingleValue) {
		Set<ORecord<?>> values = map.get(iKey);
		if (values == null)
			values = new HashSet<ORecord<?>>();

		if (!iSingleValue.getIdentity().isValid())
			iSingleValue.save();

		if (iSingleValue.getIdentity().isTemporary())
			tempItems.add(iKey);

		values.add(iSingleValue);

		map.put(iKey, values);
		return this;
	}

	public OIndex remove(final Object iKey, final ORecord<?> value) {
		final Set<ORecord<?>> recs = get(iKey);
		if (recs != null && !recs.isEmpty()) {
			if (recs.remove(value))
				map.put(iKey, recs);
		}
		return this;
	}
}
