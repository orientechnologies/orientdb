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
package com.orientechnologies.orient.server.handler.distributed;

/**
 * Cluster protocol. The range of requests is 80-110.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OClusterProtocol {
	public static final short	CURRENT_PROTOCOL_VERSION										= 0;

	public static final byte	REQUEST_NODE2NODE_CONNECT										= 80;
	public static final byte	REQUEST_LEADER2PEER_CONNECT									= 81;
	public static final byte	REQUEST_LEADER2PEER_HEARTBEAT								= 82;
	public static final byte	REQUEST_NODE2NODE_DB_COPY										= 83;
	public static final byte	REQUEST_NODE2NODE_REPLICATION_SYNCHRONIZE		= 84;
	public static final byte	REQUEST_NODE2NODE_REPLICATION_RECORD_CHANGE	= 85;

	public static final byte	PUSH_LEADER_AVAILABLE_DBS										= 100;

}