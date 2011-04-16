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
package com.orientechnologies.orient.core.db.record;

import java.util.Iterator;

public interface ORecordLazyMultiValue {
	public Iterator<OIdentifiable> rawIterator();

	/**
	 * Browse all the set to convert all the items into records.
	 */
	public void convertLinks2Records();

	/**
	 * Browse all the set to convert all the items into links.
	 * 
	 * @return
	 */
	public boolean convertRecords2Links();

	public boolean isAutoConvertToRecord();

	public void setAutoConvertToRecord(boolean convertToRecord);

}