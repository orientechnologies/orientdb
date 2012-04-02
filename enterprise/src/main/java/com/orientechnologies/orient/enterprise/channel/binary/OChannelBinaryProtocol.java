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
package com.orientechnologies.orient.enterprise.channel.binary;

import java.io.IOException;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * The range of the requests is 1-79.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OChannelBinaryProtocol {
	// OUTGOING
	public static final byte	REQUEST_SHUTDOWN							= 1;
	public static final byte	REQUEST_CONNECT								= 2;

	public static final byte	REQUEST_DB_OPEN								= 3;
	public static final byte	REQUEST_DB_CREATE							= 4;
	public static final byte	REQUEST_DB_CLOSE							= 5;
	public static final byte	REQUEST_DB_EXIST							= 6;
	public static final byte	REQUEST_DB_DROP								= 7;
	public static final byte	REQUEST_DB_SIZE								= 8;
	public static final byte	REQUEST_DB_COUNTRECORDS				= 9;

	public static final byte	REQUEST_DATACLUSTER_ADD				= 10;
	public static final byte	REQUEST_DATACLUSTER_REMOVE		= 11;
	public static final byte	REQUEST_DATACLUSTER_COUNT			= 12;
	public static final byte	REQUEST_DATACLUSTER_DATARANGE	= 13;
	public static final byte	REQUEST_DATACLUSTER_COPY			= 14;

	public static final byte	REQUEST_DATASEGMENT_ADD				= 20;
	public static final byte	REQUEST_DATASEGMENT_REMOVE		= 21;

	public static final byte	REQUEST_RECORD_LOAD						= 30;
	public static final byte	REQUEST_RECORD_CREATE					= 31;
	public static final byte	REQUEST_RECORD_UPDATE					= 32;
	public static final byte	REQUEST_RECORD_DELETE					= 33;
	public static final byte	REQUEST_RECORD_COPY						= 34;

	public static final byte	REQUEST_COUNT									= 40; // DEPRECATED: USE REQUEST_DATACLUSTER_COUNT
	public static final byte	REQUEST_COMMAND								= 41;

	public static final byte	REQUEST_TX_COMMIT							= 60;

	public static final byte	REQUEST_CONFIG_GET						= 70;
	public static final byte	REQUEST_CONFIG_SET						= 71;
	public static final byte	REQUEST_CONFIG_LIST						= 72;
	public static final byte	REQUEST_DB_RELOAD							= 73; // SINCE 1.0rc4
	public static final byte	REQUEST_DB_LIST								= 74; // SINCE 1.0rc6
	public static final byte	REQUEST_DB_COPY								= 75; // SINCE 1.0rc8
	public static final byte	REQUEST_DB_REPLICATION				= 76; // SINCE 1.0

	public static final byte	REQUEST_PUSH_RECORD						= 79;
	public static final byte	PUSH_NODE2CLIENT_DB_CONFIG		= 80;

	// INCOMING
	public static final byte	RESPONSE_STATUS_OK						= 0;
	public static final byte	RESPONSE_STATUS_ERROR					= 1;
	public static final byte	PUSH_DATA											= 3;

	// CONSTANTS
	public static final short	RECORD_NULL										= -2;
	public static final short	RECORD_RID										= -3;
	public static final int		CURRENT_PROTOCOL_VERSION			= 9;	// SENT AS SHORT AS FIRST PACKET AFTER SOCKET CONNECTION

	public static OIdentifiable readIdentifiable(final OChannelBinaryClient network) throws IOException {
		final int classId = network.readShort();
		if (classId == RECORD_NULL)
			return null;

		if (classId == RECORD_RID) {
			return network.readRID();
		} else {
			final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(network.readByte());

			if (record instanceof ORecordSchemaAware<?>)
				((ORecordSchemaAware<?>) record).fill(network.readRID(), network.readInt(), network.readBytes(), false);
			else
				// DISCARD CLASS ID
				record.fill(network.readRID(), network.readInt(), network.readBytes(), false);

			return record;
		}
	}
}
