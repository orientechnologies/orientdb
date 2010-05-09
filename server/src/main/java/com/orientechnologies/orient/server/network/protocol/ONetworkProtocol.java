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

import java.io.IOException;
import java.net.Socket;

import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.server.OClientConnection;

public abstract class ONetworkProtocol extends OSoftThread {
	protected int			totalRequests							= 0;
	protected int			commandType								= -1;
	protected int			lastCommandType						= -1;
	protected String	lastCommandDetail					= null;
	protected long		totalCommandExecutionTime	= 0;
	protected long		lastCommandExecutionTime	= 0;

	public ONetworkProtocol(ThreadGroup group, String name) {
		super(group, name);
	}

	public abstract void config(Socket iSocket, OClientConnection iConnection) throws IOException;

	public abstract OChannel getChannel();

	public int getCommandType() {
		return commandType;
	}

	public int getLastCommandType() {
		return lastCommandType;
	}

	public String getLastCommandDetail() {
		return lastCommandDetail;
	}

	public long getTotalWorkingTime() {
		return totalCommandExecutionTime;
	}

	public long getLastCommandExecutionTime() {
		return lastCommandExecutionTime;
	}

	public int getTotalRequests() {
		return totalRequests;
	}
}
