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
package com.orientechnologies.orient.server.network.protocol.binary;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerUserConfiguration;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

/**
 * Abstract base class for binary network implementations.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OBinaryNetworkProtocolAbstract extends ONetworkProtocol {
	protected OChannelBinaryServer			channel;
	protected int												requestType;
	protected int												clientTxId;
	protected OServerUserConfiguration	serverUser;
	private final Level									logClientExceptions;

	public OBinaryNetworkProtocolAbstract(final String iThreadName) {
		super(Orient.getThreadGroup(), iThreadName);
		logClientExceptions = Level.parse(OGlobalConfiguration.SERVER_LOG_CLIENT_EXCEPTION_LEVEL.getValueAsString());
	}

	/**
	 * Executes the request.
	 * 
	 * @return true if the request has been recognized, othereise false
	 * @throws IOException
	 */
	protected abstract boolean executeRequest() throws IOException;

	/**
	 * Executed before the request.
	 * 
	 * @throws IOException
	 */
	protected void onBeforeRequest() throws IOException {
	}

	/**
	 * Executed after the request, also in case of error.
	 * 
	 * @throws IOException
	 */
	protected void onAfterRequest() throws IOException {
	}

	@Override
	public void config(final OServer iServer, final Socket iSocket, final OContextConfiguration iConfig) throws IOException {
		server = iServer;
		channel = new OChannelBinaryServer(iSocket, iConfig);
	}

	@Override
	protected void execute() throws Exception {
		requestType = -1;

		clientTxId = 0;

		try {
			requestType = channel.readByte();
			clientTxId = channel.readInt();

			onBeforeRequest();

			if (!executeRequest()) {
				OLogManager.instance().error(this, "Request not supported. Code: " + requestType);
				channel.clearInput();
				sendError(clientTxId, new ONetworkProtocolException("Request not supported. Code: " + requestType));
			}

			onAfterRequest();

		} catch (EOFException e) {
			handleConnectionError(channel, e);
			sendShutdown();
		} catch (SocketException e) {
			handleConnectionError(channel, e);
			sendShutdown();
		} catch (OException e) {
			sendError(clientTxId, e);
		} catch (RuntimeException e) {
			sendError(clientTxId, e);
		} catch (Throwable t) {
			OLogManager.instance().error(this, "Error on executing request", t);
			sendError(clientTxId, t);
		} finally {

			OSerializationThreadLocal.INSTANCE.get().clear();
		}
	}

	@Override
	public void shutdown() {
		channel.close();
	}

	@Override
	public OChannel getChannel() {
		return channel;
	}

	protected void sendOk(final int iClientTxId) throws IOException {
		channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
		channel.writeInt(iClientTxId);
	}

	protected void sendError(final int iClientTxId, final Throwable t) throws IOException {
		channel.acquireExclusiveLock();

		try {
			channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_ERROR);
			channel.writeInt(iClientTxId);

			Throwable current;
			if (t instanceof OLockException && t.getCause() instanceof ODatabaseException)
				// BYPASS THE DB POOL EXCEPTION TO PROPAGATE THE RIGHT SECURITY ONE
				current = t.getCause();
			else
				current = t;

			while (current != null) {
				// MORE DETAILS ARE COMING AS EXCEPTION
				channel.writeByte((byte) 1);

				channel.writeString(current.getClass().getName());
				channel.writeString(current != null ? current.getMessage() : null);

				current = current.getCause();
			}
			channel.writeByte((byte) 0);

			channel.flush();

			if (OLogManager.instance().isLevelEnabled(logClientExceptions))
				OLogManager.instance().log(this, logClientExceptions, "Sent run-time exception to the client %s", current,
						channel.socket.getRemoteSocketAddress());

		} finally {
			channel.releaseExclusiveLock();
		}
	}

	/**
	 * Write a OIdentifiable instance using this format:<br/>
	 * - 2 bytes: class id [-2=no record, -3=rid, -1=no class id, > -1 = valid] <br/>
	 * - 1 byte: record type [d,b,f] <br/>
	 * - 2 bytes: cluster id <br/>
	 * - 8 bytes: position in cluster <br/>
	 * - 4 bytes: record version <br/>
	 * - x bytes: record content <br/>
	 * 
	 * @param o
	 * @throws IOException
	 */
	public void writeIdentifiable(final OIdentifiable o) throws IOException {
		if (o == null)
			channel.writeShort(OChannelBinaryProtocol.RECORD_NULL);
		else if (o instanceof ORecordId) {
			channel.writeShort(OChannelBinaryProtocol.RECORD_RID);
			channel.writeRID((ORID) o);
		} else {
			writeRecord((ORecordInternal<?>) o.getRecord());
		}
	}

	private void writeRecord(final ORecordInternal<?> iRecord) throws IOException {
		channel.writeShort((short) 0);
		channel.writeByte(iRecord.getRecordType());
		channel.writeRID(iRecord.getIdentity());
		channel.writeInt(iRecord.getVersion());
		try {
			final byte[] stream = iRecord.toStream();

			// TRIM TAILING SPACES (DUE TO OVERSIZE)
			int realLength = stream.length;
			for (int i = stream.length - 1; i > -1; --i) {
				if (stream[i] == 32)
					--realLength;
				else
					break;
			}

			channel.writeBytes(stream, realLength);
		} catch (Exception e) {
			channel.writeBytes(null);
			OLogManager.instance().error(this, "Error on unmarshalling record " + iRecord.getIdentity().toString(),
					OSerializationException.class);
		}
	}

	protected void checkStorageExistence(final String iDatabaseName) {
		for (OStorage stg : Orient.instance().getStorages()) {
			if (stg.getName().equalsIgnoreCase(iDatabaseName) && stg.exists())
				throw new ODatabaseException("Database named '" + iDatabaseName + "' already exists: " + stg);
		}
	}

	protected ODatabaseDocumentTx createDatabase(final ODatabaseDocumentTx iDatabase, String dbUser, final String dbPasswd) {
		if (iDatabase.exists())
			throw new ODatabaseException("Database '" + iDatabase.getURL() + "' already exists");

		iDatabase.create();
		if (dbUser != null) {

			OUser oUser = iDatabase.getMetadata().getSecurity().getUser(dbUser);
			if (oUser == null) {
				iDatabase.getMetadata().getSecurity().createUser(dbUser, dbPasswd, new String[] { ORole.ADMIN });
			} else {
				oUser.setPassword(dbPasswd);
				oUser.save();
			}
		}

		OLogManager.instance().info(this, "Created database '%s' of type '%s'", iDatabase.getURL(),
				iDatabase.getStorage() instanceof OStorageLocal ? "local" : "memory");

		if (iDatabase.getStorage() instanceof OStorageLocal)
			// CLOSE IT BECAUSE IT WILL BE OPEN AT FIRST USE
			iDatabase.close();

		return iDatabase;
	}

	protected ODatabaseDocumentTx getDatabaseInstance(final String iDbName, final String iDbType, final String iStorageType) {
		final String path;

		final OStorage stg = Orient.instance().getStorage(iDbName);
		if (stg != null)
			path = stg.getURL();
		else if (iStorageType.equals(OEngineLocal.NAME)) {
			path = iStorageType + ":${ORIENTDB_HOME}/databases/" + iDbName;
		} else if (iStorageType.equals(OEngineMemory.NAME)) {
			path = iStorageType + ":" + iDbName;
		} else
			throw new IllegalArgumentException("Cannot create database: storage mode '" + iStorageType + "' is not supported.");

		return Orient.instance().getDatabaseFactory().createDatabase(iDbType, path);
	}

	protected void checkServerAccess(final String iResource) {
		if (serverUser == null)
			throw new OSecurityAccessException("Server user not authenticated.");

		if (!OServerMain.server().authenticate(serverUser.name, null, iResource))
			throw new OSecurityAccessException("User '" + serverUser.name + "' cannot access to the resource [" + iResource
					+ "]. Use another server user or change permission in the file config/orientdb-server-config.xml");
	}

	protected OServerUserConfiguration serverLogin(final String iUser, final String iPassword, final String iResource) {
		if (!OServerMain.server().authenticate(iUser, iPassword, iResource))
			throw new OSecurityAccessException(
					"Wrong user/password to [connect] to the remote OrientDB Server instance. Get the user/password from the config/orientdb-server-config.xml file");

		serverUser = OServerMain.server().getUser(iUser);
		return serverUser;
	}

	protected ODatabaseComplex<?> openDatabase(final String iDbType, final String iDbUrl, final String iUser, final String iPassword) {
		final String path = OServerMain.server().getStoragePath(iDbUrl);

		final ODatabaseComplex<?> database = Orient.instance().getDatabaseFactory().createDatabase(iDbType, path);

		if (database.isClosed())
			if (database.getStorage() instanceof OStorageMemory)
				database.create();
			else {
				try {
					database.open(iUser, iPassword);
				} catch (OSecurityException e) {
					// TRY WITH SERVER'S USER
					try {
						serverLogin(iUser, iPassword, "database.passthrough");
					} catch (OSecurityException ex) {
						throw e;
					}

					// SERVER AUTHENTICATED, BYPASS SECURITY
					database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
					database.open(iUser, iPassword);
				}
			}

		return database;
	}

	protected ODatabaseComplex<?> openDatabase(final ODatabaseComplex<?> database, final String iUser, final String iPassword) {

		if (database.isClosed())
			if (database.getStorage() instanceof OStorageMemory)
				database.create();
			else {
				try {
					database.open(iUser, iPassword);
				} catch (OSecurityException e) {
					// TRY WITH SERVER'S USER
					try {
						serverLogin(iUser, iPassword, "database.passthrough");
					} catch (OSecurityException ex) {
						throw e;
					}

					// SERVER AUTHENTICATED, BYPASS SECURITY
					database.setProperty(ODatabase.OPTIONS.SECURITY.toString(), Boolean.FALSE);
					database.open(iUser, iPassword);
				}
			}

		return database;
	}

	protected int deleteRecord(final ODatabaseRecord iDatabase, final ORID rid, final int version) {
		final ORecordInternal<?> record = iDatabase.load(rid);
		if (record != null) {
			record.setVersion(version);
			record.delete();
			return 1;
		}
		return 0;
	}

	protected long createRecord(final ODatabaseRecord iDatabase, final ORecordId rid, final byte[] buffer, final byte recordType) {
		final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
		record.fill(rid, 0, buffer, true);
		iDatabase.save(record);
		return record.getIdentity().getClusterPosition();
	}

	protected int updateRecord(final ODatabaseRecord iDatabase, final ORecordId rid, final byte[] buffer, final int version,
			final byte recordType) {
		final ORecordInternal<?> newRecord = Orient.instance().getRecordFactoryManager().newInstance(recordType);
		newRecord.fill(rid, version, buffer, true);

//		if (((OSchemaProxy) iDatabase.getMetadata().getSchema()).getIdentity().equals(rid))
//			// || ((OIndexManagerImpl) connection.database.getMetadata().getIndexManager()).getDocument().getIdentity().equals(rid)) {
//			throw new OSecurityAccessException("Cannot update internal record " + rid);

		final ORecordInternal<?> currentRecord;
		if (newRecord instanceof ODocument) {
			currentRecord = iDatabase.load(rid);

			if (currentRecord == null)
				throw new ORecordNotFoundException(rid.toString());

			((ODocument) currentRecord).merge((ODocument) newRecord, false, false);

		} else
			currentRecord = newRecord;

		currentRecord.setVersion(version);

		iDatabase.save(currentRecord);

		if (currentRecord.getIdentity().toString().equals(iDatabase.getStorage().getConfiguration().indexMgrRecordId)
				&& !iDatabase.getStatus().equals(STATUS.IMPORTING)) {
			// FORCE INDEX MANAGER UPDATE. THIS HAPPENS FOR DIRECT CHANGES FROM REMOTE LIKE IN GRAPH
			iDatabase.getMetadata().getIndexManager().reload();
		}
		return currentRecord.getVersion();
	}

	protected void handleConnectionError(final OChannelBinaryServer channel, final Throwable e) {
		try {
			channel.flush();
		} catch (IOException e1) {
		}
	}
}
