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

import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Contains the information about a database operation.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordOperation {

	public static final byte	LOADED	= 0;
	public static final byte	UPDATED	= 1;
	public static final byte	DELETED	= 2;
	public static final byte	CREATED	= 3;

	public byte								type;
	public OIdentifiable			record;
	public String							clusterName;

	public ORecordOperation() {
	}

	public ORecordOperation(final ORecordInternal<?> iRecord, final byte iStatus, final String iClusterName) {
		// CLONE RECORD AND CONTENT
		this.record = iRecord;
		this.type = iStatus;
		this.clusterName = iClusterName;
	}

	@Override
	public int hashCode() {
		return record.getIdentity().hashCode();
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof ORecordOperation))
			return false;

		return record.equals(((ORecordOperation) obj).record);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		builder.append("OTransactionEntry [record=").append(record).append(", status=").append(type).append(", clusterName=")
				.append(clusterName).append("]");
		return builder.toString();
	}

	public ORecordInternal<?> getRecord() {
		return (ORecordInternal<?>) (record != null ? record.getRecord() : null);
	}
}
