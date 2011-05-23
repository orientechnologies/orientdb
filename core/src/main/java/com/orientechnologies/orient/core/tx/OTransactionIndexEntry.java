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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

public class OTransactionIndexEntry {
	public static enum STATUSES {
		PUT, REMOVE, CLEAR
	};

	public STATUSES				status;
	public Object					key;
	public OIdentifiable	value;

	public OTransactionIndexEntry(final STATUSES iStatus, final Object iKey, final OIdentifiable iValue) {
		this.status = iStatus;
		this.key = iKey;
		this.value = iValue;
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder();
		builder.append(key).append(':').append(value).append(" = ").append(status);
		return builder.toString();
	}
}
