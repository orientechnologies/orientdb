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
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionaryInternal;
import com.orientechnologies.orient.core.dictionary.ODictionaryIterator;
import com.orientechnologies.orient.core.record.ORecordInternal;

@SuppressWarnings("unchecked")
public class ODictionaryMemory<T extends Object> implements ODictionaryInternal<T> {
	private HashMap<String, T>	underlying	= new HashMap<String, T>();

	public ODictionaryMemory(final ODatabaseRecord iDatabase) {
	}

	public void create() {
	}

	public void load() {
	}

	public <RET extends Object> RET get(final Object iKey) {
		return (RET) underlying.get(iKey);
	}

	public <RET extends Object> RET get(final Object iKey, final String iFetchPlan) {
		return (RET) underlying.get(iKey);
	}

	public Iterator<Entry<String, T>> iterator() {
		return new ODictionaryIterator<T>(underlying);
	}

	public ORecordInternal<?> putRecord(final String iKey, final ORecordInternal<?> iValue) {
		return (ORecordInternal<?>) underlying.put(iKey, (T) iValue);
	}

	public <RET extends Object> RET put(final String iKey, final Object iValue) {
		return (RET) underlying.put(iKey, (T) iValue);
	}

	public boolean containsKey(Object iKey) {
		return underlying.containsKey(iKey);
	}

	public <RET extends Object> RET remove(final Object iKey) {
		return (RET) underlying.remove(iKey);
	}

	public int size() {
		return underlying.size();
	}

	public Set<String> keySet() {
		return underlying.keySet();
	}

}
