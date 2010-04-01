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

import com.orientechnologies.orient.core.record.ORecordInternal;

public class OTransactionEntry<REC extends ORecordInternal<?>> {

	public static final byte	LOADED	= 0;
	public static final byte	UPDATED	= 1;
	public static final byte	DELETED	= 2;
	public static final byte	CREATED	= 3;

	public byte								status;
	public REC								record;
	public String							clusterName;

	public OTransactionEntry(final REC iRecord, final byte iStatus, final String iClusterName) {
		this.record = iRecord;
		this.status = iStatus;
		this.clusterName = iClusterName;
	}
}
