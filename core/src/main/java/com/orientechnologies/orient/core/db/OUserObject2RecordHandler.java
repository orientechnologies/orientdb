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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Basic interface to handle the mapping between user objects and records. In some database implementation the user objects can be
 * the records themselves.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OUserObject2RecordHandler {
	/**
	 * Returns the record associated to a user object. If iCreateIfNotAvailable is true, then a new record instance will be created
	 * transparently.
	 * 
	 * @param iUserObject
	 *          User object
	 * @param iCreateIfNotAvailable
	 *          Create the record if not available
	 * @return The record associated
	 */
	public ORecordInternal<?> getRecordByUserObject(Object iUserObject, boolean iCreateIfNotAvailable);

	/**
	 * Returns the user object associated to a record. If the record is not loaded yet, iFetchPlan will be used as fetch plan.
	 * 
	 * @param iRecord
	 *          Record
	 * @param iFetchPlan
	 *          If the record is not loaded yet, use this as fetch plan
	 * @return The user object associated
	 */
	public Object getUserObjectByRecord(ORecordInternal<?> iRecord, String iFetchPlan);

	/**
	 * Tells if e user object exists for a certain RecordId.
	 */
	public boolean existsUserObjectByRID(ORID iRID);

	/**
	 * Registers the association between a user object and a record.
	 * 
	 * @param iUserObject
	 *          User object
	 * @param iRecord
	 *          record
	 */
	public void registerUserObject(final Object iUserObject, final ORecordInternal<?> iRecord);
}
