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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionaryInternal;
import com.orientechnologies.orient.core.dictionary.ODictionaryIterator;
import com.orientechnologies.orient.core.record.ORecordInternal;

@SuppressWarnings("serial")
public class ODictionaryMemory<T extends Object> extends HashMap<String, T> implements ODictionaryInternal<T> {

	public ODictionaryMemory(final ODatabaseRecord<?> iDatabase) {
	}

	public void create() {
	}

	public void load() {
	}

	public T get(final Object iKey, final String iFetchPlan) {
		return get(iKey);
	}

	public Iterator<Entry<String, T>> iterator() {
		return new ODictionaryIterator<T>(this);
	}

	@SuppressWarnings("unchecked")
	public ORecordInternal<?> putRecord(final String iKey, final ORecordInternal<?> iValue) {
		return (ORecordInternal<?>) put(iKey, (T) iValue);
	}
}
