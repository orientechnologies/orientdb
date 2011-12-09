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
package com.orientechnologies.orient.core.db.record;

import java.util.Iterator;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Implementation of ArrayList bound to a source ORecord object to keep track of changes. This avoid to call the makeDirty() by hand
 * when the list is changed.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "serial" })
public class ORecordTrackedList extends OTrackedList<OIdentifiable> {
	public ORecordTrackedList(final ORecordInternal<?> iSourceRecord) {
		super(iSourceRecord);
	}

	public Iterator<OIdentifiable> rawIterator() {
		return iterator();
	}

	/**
	 * The item's identity does not affect nothing.
	 */
	public void onBeforeIdentityChanged(final ORID iRID) {
	}

	/**
	 * The item's identity does not affect nothing.
	 */
	public void onAfterIdentityChanged(final ORecord<?> iRecord) {
	}
}
