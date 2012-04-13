/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.replication;

import java.io.IOException;
import java.util.logging.Level;

import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;

/**
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedDatabaseInfo {

	public enum SYNCH_TYPE {
		SYNCH, ASYNCH
	}

	public enum STATUS_TYPE {
		ONLINE, OFFLINE, SYNCHRONIZING
	}

	public String									serverId;
	public String									databaseName;
	public String									userName;
	public String									userPassword;
	public SYNCH_TYPE							synchType;
	public ONodeConnection				connection;
	public STATUS_TYPE						status;
	private OOperationLog					log;
	private final OClusterLogger	logger	= new OClusterLogger();

	public ODistributedDatabaseInfo(final String iServerid, final String dbName, final String iUserName, final String iUserPasswd,
			final SYNCH_TYPE iSynchType, final STATUS_TYPE iStatus) throws IOException {
		serverId = iServerid;
		databaseName = dbName;
		userName = iUserName;
		userPassword = iUserPasswd;
		synchType = iSynchType;
		status = iStatus;

		logger.setDatabase(dbName);
		logger.setNode(iServerid);

		log = new OOperationLog(serverId, databaseName, false);
	}

	public void close() throws IOException {
		if (log != null)
			log.close();
		setStatus(STATUS_TYPE.OFFLINE);
	}

	public boolean isOnline() {
		return status == STATUS_TYPE.ONLINE;
	}

	public void setOnline() {
		setStatus(STATUS_TYPE.ONLINE);
	}

	public void setSynchronizing() {
		setStatus(STATUS_TYPE.SYNCHRONIZING);
	}

	public void setOffline() {
		setStatus(STATUS_TYPE.OFFLINE);
	}

	public OOperationLog getLog() {
		return log;
	}

	private void setStatus(final STATUS_TYPE iStatus) {
		logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE, "distributed db changed status %s -> %s", status, iStatus);
		status = iStatus;
	}
}
