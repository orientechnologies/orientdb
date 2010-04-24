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

import java.util.Iterator;

import com.orientechnologies.common.collection.OTreeMapMemory;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionaryInternal;
import com.orientechnologies.orient.core.dictionary.ODictionaryIterator;

@SuppressWarnings("serial")
public class ODictionaryMemory<T extends Object> extends OTreeMapMemory<String, T> implements ODictionaryInternal<T> {

	private ODatabaseRecord<?>	database;

	public ODictionaryMemory(final ODatabaseRecord<?> iDatabase) {
		database = iDatabase;
	}

	public void create() {
	}

	public void load() {
	}

	public Iterator<Entry<String, T>> iterator() {
		return new ODictionaryIterator<T>(this);
	}
}
