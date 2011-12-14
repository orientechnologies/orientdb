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
package com.orientechnologies.orient.server.replication.conflict;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Exception thrown when the two servers are not aligned.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OReplicationConflictException extends OException {

	private static final String	MESSAGE_RECORD_VERSION	= "your=v";

	private static final String	MESSAGE_DB_VERSION			= "db=v";

	private static final long		serialVersionUID				= 1L;

	private final ORID					rid;
	private final int						databaseVersion;
	private final int						recordVersion;

	/**
	 * Rebuilds the original exception from the message.
	 */
	public OReplicationConflictException(final String message) {
		super(message);
		int beginPos = message.indexOf(ORID.PREFIX);
		int endPos = message.indexOf(' ', beginPos);
		rid = new ORecordId(message.substring(beginPos, endPos));

		beginPos = message.indexOf(MESSAGE_DB_VERSION, endPos) + MESSAGE_DB_VERSION.length();
		endPos = message.indexOf(' ', beginPos);
		databaseVersion = Integer.parseInt(message.substring(beginPos, endPos));

		beginPos = message.indexOf(MESSAGE_RECORD_VERSION, endPos) + MESSAGE_RECORD_VERSION.length();
		endPos = message.indexOf(')', beginPos);
		recordVersion = Integer.parseInt(message.substring(beginPos, endPos));
	}

	public OReplicationConflictException(final String message, final ORID iRID, final int iDatabaseVersion, final int iRecordVersion) {
		super(message);
		rid = iRID;
		databaseVersion = iDatabaseVersion;
		recordVersion = iRecordVersion;
	}

	public int getDatabaseVersion() {
		return databaseVersion;
	}

	public int getRecordVersion() {
		return recordVersion;
	}

	public ORID getRid() {
		return rid;
	}
}
