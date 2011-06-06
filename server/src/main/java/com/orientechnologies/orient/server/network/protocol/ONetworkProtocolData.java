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
package com.orientechnologies.orient.server.network.protocol;

/**
 * Saves all the important information about the network connection. Useful for monitoring and statistics.
 * 
 * @author Luca Garulli
 * 
 */
public class ONetworkProtocolData {
	public int		totalRequests							= 0;
	public String	commandInfo								= null;
	public String	commandDetail							= null;
	public String	lastCommandInfo						= null;
	public String	lastCommandDetail					= null;
	public long		lastCommandExecutionTime	= 0;
	public long		lastCommandReceived				= 0;
	public long		totalCommandExecutionTime	= 0;
	public String	serverInfo								= null;
	public String	caller										= null;
}
