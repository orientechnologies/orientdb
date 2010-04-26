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
package com.orientechnologies.orient.core.query;

import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

public interface OQuery<T extends ORecordInternal<?>> {

	/**
	 * Execute the query without limit about the result set. The limit will be bound to the maximum allowed.
	 * 
	 * @return List of records if any record matches the query constraints, otherwise an empty List.
	 */
	public List<T> execute();

	/**
	 * Execute the query setting a limit of results.
	 * 
	 * @param iLimit
	 *          -1 = the limit will be bound to the maximum allowed, otherwise set the limit of the result.
	 * @return List of records if any record matches the query constraints, otherwise an empty List. The List can be of maximum iLimit
	 *         elements
	 */
	public List<T> execute(int iLimit);

	/**
	 * Return the first occurrence found if any
	 * 
	 * @return Record if found, otherwise null
	 */
	public T executeFirst();

	public ODatabaseRecord<T> getDatabase();
}
