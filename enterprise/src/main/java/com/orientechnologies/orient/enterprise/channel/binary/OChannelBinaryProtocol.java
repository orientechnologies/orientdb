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

/**
 * The range of the requests is 1-79.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OChannelBinaryProtocol {
	// REQUESTS
	public static final byte	REQUEST_SHUTDOWN							= 1;
	public static final byte	REQUEST_CONNECT								= 2;

	public static final byte	REQUEST_DB_OPEN								= 5;
	public static final byte	REQUEST_DB_CREATE							= 6;
	public static final byte	REQUEST_DB_CLOSE							= 7;
	public static final byte	REQUEST_DB_EXIST							= 8;
	public static final byte	REQUEST_DB_DELETE							= 9;

	public static final byte	REQUEST_DATACLUSTER_ADD				= 10;
	public static final byte	REQUEST_DATACLUSTER_REMOVE		= 11;
	public static final byte	REQUEST_DATACLUSTER_COUNT			= 12;
	public static final byte	REQUEST_DATACLUSTER_DATARANGE	= 13;

	public static final byte	REQUEST_DATASEGMENT_ADD				= 20;
	public static final byte	REQUEST_DATASEGMENT_REMOVE		= 21;

	public static final byte	REQUEST_RECORD_LOAD						= 30;
	public static final byte	REQUEST_RECORD_CREATE					= 31;
	public static final byte	REQUEST_RECORD_UPDATE					= 32;
	public static final byte	REQUEST_RECORD_DELETE					= 33;

	public static final byte	REQUEST_COUNT									= 40;
	public static final byte	REQUEST_COMMAND								= 41;

	public static final byte	REQUEST_DICTIONARY_LOOKUP			= 50;
	public static final byte	REQUEST_DICTIONARY_PUT				= 51;
	public static final byte	REQUEST_DICTIONARY_REMOVE			= 52;
	public static final byte	REQUEST_DICTIONARY_SIZE				= 53;
	public static final byte	REQUEST_DICTIONARY_KEYS				= 54;

	public static final byte	REQUEST_TX_COMMIT							= 60;

	public static final byte	REQUEST_CONFIG_GET						= 70;
	public static final byte	REQUEST_CONFIG_SET						= 71;
	public static final byte	REQUEST_CONFIG_LIST						= 72;

	// RESPONSES
	public static final byte	RESPONSE_STATUS_OK						= 0;
	public static final byte	RESPONSE_STATUS_ERROR					= 1;

	// CONSTANTS
	public static final int		RECORD_NULL										= -2;
	public static final int		CURRENT_PROTOCOL_VERSION			= 0;	// NOT YET USED
}
