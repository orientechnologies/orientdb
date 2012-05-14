/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

	private static final String	MESSAGE_REMOTE_VERSION	= "remote=v";
	private static final String	MESSAGE_LOCAL_VERSION		= "local=v";

	private static final long		serialVersionUID				= 1L;

	private final ORID					localRID;
	private final int						localVersion;
	private final ORID					remoteRID;
	private final int						remoteVersion;

	/**
	 * Rebuilds the original exception from the message.
	 */
	public OReplicationConflictException(final String message) {
		super(message);
		int beginPos = message.indexOf(ORID.PREFIX);
		int endPos = message.indexOf(' ', beginPos);
		localRID = new ORecordId(message.substring(beginPos, endPos));

		beginPos = message.indexOf(MESSAGE_LOCAL_VERSION, endPos) + MESSAGE_LOCAL_VERSION.length();
		endPos = message.indexOf(' ', beginPos);
		localVersion = Integer.parseInt(message.substring(beginPos, endPos));

		beginPos = message.indexOf(MESSAGE_REMOTE_VERSION, endPos) + MESSAGE_REMOTE_VERSION.length();
		endPos = message.indexOf(')', beginPos);
		remoteVersion = Integer.parseInt(message.substring(beginPos, endPos));
		remoteRID = null;
	}

	public OReplicationConflictException(final String message, final ORID iRID, final int iDatabaseVersion, final int iRecordVersion) {
		super(message);
		localRID = iRID;
		remoteRID = null;
		localVersion = iDatabaseVersion;
		remoteVersion = iRecordVersion;
	}

	public OReplicationConflictException(final String message, final ORID iOriginalRID, final ORID iRemoteRID) {
		super(message);
		localRID = iOriginalRID;
		remoteRID = iRemoteRID;
		localVersion = remoteVersion = 0;
	}

	@Override
	public String getMessage() {
		final StringBuilder buffer = new StringBuilder(super.getMessage());

		if (remoteRID != null) {
			// RID CONFLICT
			buffer.append("local RID=");
			buffer.append(localRID);
			buffer.append(" remote RID=");
			buffer.append(remoteRID);
		} else {
			// VERSION CONFLICT
			buffer.append("local=v");
			buffer.append(localVersion);
			buffer.append(" remote=v");
			buffer.append(remoteVersion);
		}

		return buffer.toString();
	}

	@Override
	public String toString() {
		return getMessage();
	}

	public int getLocalVersion() {
		return localVersion;
	}

	public int getRemoteVersion() {
		return remoteVersion;
	}

	public ORID getLocalRID() {
		return localRID;
	}

	public ORID getRemoteRID() {
		return remoteRID;
	}
}
