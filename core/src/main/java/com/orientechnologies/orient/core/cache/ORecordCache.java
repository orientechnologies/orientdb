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
package com.orientechnologies.orient.core.cache;

import java.util.LinkedHashMap;
import java.util.Map;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Cache of records.
 * 
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("serial")
public class ORecordCache extends LinkedHashMap<ORID, ORecordInternal<?>> {
	private int	maxSize;

	public ORecordCache(final int maxSize, final int initialCapacity, final float loadFactor) {
		super(initialCapacity, loadFactor, true);
		this.maxSize = maxSize;
	}

	@Override
	protected boolean removeEldestEntry(final Map.Entry<ORID, ORecordInternal<?>> iEldest) {
		final int size = size();
		if (maxSize == -1 || size < maxSize)
			// DON'T REMOVE ELDEST
			return false;

		if (maxSize - size > 1) {
			// REMOVE ITEMS MANUALLY
			removeEldestItems(maxSize - size);
			return false;
		} else
			return true;
	}

	public void removeEldestItems(final int iThreshold) {
		// CLEAR THE CACHE: USE A TEMP ARRAY TO AVOID ITERATOR EXCEPTIONS
		final ORID[] ridToRemove = new ORID[size() - iThreshold];

		int entryNum = 0;
		int i = 0;
		for (java.util.Map.Entry<ORID, ORecordInternal<?>> ridEntry : entrySet()) {
			if (!ridEntry.getValue().isDirty())
				if (entryNum++ >= iThreshold)
					// ADD ONLY AFTER THRESHOLD. THIS IS TO GET THE LESS USED
					ridToRemove[i++] = ridEntry.getKey();

			if (i >= ridToRemove.length)
				break;
		}

		for (ORID rid : ridToRemove)
			remove(rid);
	}
}
