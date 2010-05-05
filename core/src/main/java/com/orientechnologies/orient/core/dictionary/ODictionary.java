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
package com.orientechnologies.orient.core.dictionary;

import java.util.Set;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Main TreeMap to handle pairs of Key/Values. This is the entry point for all the root objects.<br/>
 * <br/>
 * Usage:<br/>
 * <br/>
 * // BIND WITH NAME 'company'<br/>
 * db.getDictionary().put("company", company );<br/>
 * <br/>
 * // RETRIEVE IT<br/>
 * company = db.getDictionary().get("company");
 * 
 * @author Luca Garulli
 * 
 */
public interface ODictionary<T extends Object> extends Iterable<Entry<String, T>> {
	/**
	 * Get a record by its key.
	 * 
	 * @param iKey
	 *          Key to search
	 * @return The Record if found, otherwise null
	 */
	public T get(Object iKey);

	/**
	 * Put a new association between the iKey and the iValue. If the association already exists, replace it with the new one and
	 * return the previous value.
	 * 
	 * @param iKey
	 *          Key to bind
	 * @param iValue
	 *          Value to bind.
	 * @return The previous value if any, otherwise null
	 */
	public T put(String iKey, T iValue);

	/**
	 * Check if the dictionary contains a key.
	 * 
	 * @param iKey
	 *          Key to search
	 * @return True if found, otherwise false
	 */
	public boolean containsKey(Object iKey);

	/**
	 * Remove an entry if exists.
	 * 
	 * @param iKey
	 *          Key to remove
	 * @return The Value associated with the key if found, otherwise null
	 */
	public T remove(Object iKey);

	/**
	 * Return the total number of elements in the dictionary.
	 */
	public int size();

	/**
	 * Return the set of all the keys.
	 */
	public Set<String> keySet();

	public ORecordInternal<?> putRecord(String iKey, ORecordInternal<?> iValue);
}
