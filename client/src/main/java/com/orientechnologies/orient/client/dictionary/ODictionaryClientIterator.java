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
package com.orientechnologies.orient.client.dictionary;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

@SuppressWarnings("unchecked")
public class ODictionaryClientIterator<T> implements Iterator<Entry<String, T>> {
	private ODatabaseRecord<?>	database;
	private List<String>				keys		= new ArrayList<String>();
	private int									cursor	= -1;

	public ODictionaryClientIterator(final ODatabaseRecord<?> iDatabase, final Set<String> iKeys) {
		this.database = iDatabase;
		this.keys.addAll(iKeys);
	}

	public boolean hasNext() {
		return cursor + 1 < keys.size();
	}

	public Entry<String, T> next() {
		String key = keys.get(++cursor);
		return (Entry<String, T>) new OPair(key, database.getDictionary().get(key));
	}

	public void remove() {
		String key = keys.get(cursor);
		database.getDictionary().remove(key);
	}
}
