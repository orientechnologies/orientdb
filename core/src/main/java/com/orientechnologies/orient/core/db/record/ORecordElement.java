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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Base interface that represents a record element.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface ORecordElement {
	/**
	 * Available record statuses.
	 */
	public enum STATUS {
		NOT_LOADED, LOADED, MARSHALLING, UNMARSHALLING
	}

	/**
	 * Returns the current status of the record.
	 * 
	 * @return Current status as value between the defined values in the enum {@link STATUS}
	 */
	public STATUS getInternalStatus();

	/**
	 * Changes the current internal status.
	 * 
	 * @param iStatus
	 *          status between the values defined in the enum {@link STATUS}
	 */
	public void setInternalStatus(STATUS iStatus);

	/**
	 * Marks the instance as dirty. The dirty status could be propagated up if the implementation supports ownership concept.
	 * 
	 * @return The object it self. Useful to call methods in chain.
	 */
	public <RET> RET setDirty();

	/**
	 * Internal only.
	 */
	public void onBeforeIdentityChanged(ORID iRID);

	/**
	 * Internal only.
	 */
	public void onAfterIdentityChanged(ORecord<?> iRecord);
}
