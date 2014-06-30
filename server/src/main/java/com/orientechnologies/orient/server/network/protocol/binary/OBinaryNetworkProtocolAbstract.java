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
package com.orientechnologies.orient.server.network.protocol.binary;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.OChannel;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.enterprise.channel.binary.ONetworkProtocolException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.ONetworkProtocol;

/**
 * Abstract base class for binary network implementations.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OBinaryNetworkProtocolAbstract extends ONetworkProtocol {
  protected final Level          logClientExceptions;
  protected final boolean        logClientFullStackTrace;
  protected OChannelBinaryServer channel;
  protected int                  requestType;
  protected int                  clientTxId;

  public OBinaryNetworkProtocolAbstract(final String iThreadName) {
    super(Orient.instance().getThreadGroup(), iThreadName);
    logClientExceptions = Level.parse(OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_LEVEL.getValueAsString());
    logClientFullStackTrace = OGlobalConfiguration.SERVER_LOG_DUMP_CLIENT_EXCEPTION_FULLSTACKTRACE.getValueAsBoolean();
  }

  @Override
  public void config(final OServerNetworkListener iListener, final OServer iServer, final Socket iSocket,
      final OContextConfiguration iConfig) throws IOException {
    server = iServer;
    channel = new OChannelBinaryServer(iSocket, iConfig);
  }

  @Override
  public int getVersion() {
    return OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION;
  }

  @Override
  public void shutdown() {
    channel.close();
  }

  @Override
  public OChannel getChannel() {
    return channel;
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

  public String getType() {
    return "binary";
  }

  /**
   * Executes the request.
   * 
   * @return true if the request has been recognized, otherwise false
   * @throws IOException
   */
  protected abstract boolean executeRequest() throws IOException;

  protected abstract void sendError(final int iClientTxId, final Throwable t) throws IOException;

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
  protected void execute() throws Exception {
    requestType = -1;

    clientTxId = 0;

    long timer = 0;

    try {
      requestType = channel.readByte();
      clientTxId = channel.readInt();

      timer = Orient.instance().getProfiler().startChrono();

      onBeforeRequest();

      try {
        if (!executeRequest()) {
          OLogManager.instance().error(this, "Request not supported. Code: " + requestType);
          channel.clearInput();
          sendError(clientTxId, new ONetworkProtocolException("Request not supported. Code: " + requestType));
        }
      } finally {
        onAfterRequest();
      }

    } catch (IOException e) {
      handleConnectionError(channel, e);
      sendShutdown();
    } catch (OException e) {
      sendError(clientTxId, e);
    } catch (RuntimeException e) {
      sendError(clientTxId, e);
    } catch (Throwable t) {
      sendError(clientTxId, t);
    } finally {
      Orient.instance().getProfiler()
          .stopChrono("server.network.requests", "Total received requests", timer, "server.network.requests");

      OSerializationThreadLocal.INSTANCE.get().clear();
    }
  }

  protected void sendOk(final int iClientTxId) throws IOException {
    channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
    channel.writeInt(iClientTxId);
  }

  protected void checkStorageExistence(final String iDatabaseName) {
    for (OStorage stg : Orient.instance().getStorages()) {
      if (!(stg instanceof OStorageProxy) && stg.getName().equalsIgnoreCase(iDatabaseName) && stg.exists())
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

    OLogManager.instance().info(this, "Created database '%s' of type '%s'", iDatabase.getName(),
        iDatabase.getStorage() instanceof OStorageLocalAbstract ? iDatabase.getStorage().getType() : "memory");

    // if (iDatabase.getStorage() instanceof OStorageLocal)
    // // CLOSE IT BECAUSE IT WILL BE OPEN AT FIRST USE
    // iDatabase.close();

    return iDatabase;
  }

  protected ODatabaseDocumentTx getDatabaseInstance(final String dbName, final String dbType, final String storageType) {
    String path;

    final OStorage stg = Orient.instance().getStorage(dbName);
    if (stg != null)
      path = stg.getURL();
    else if (storageType.equals(OEngineLocalPaginated.NAME)) {
      // if this storage was configured return always path from config file, otherwise return default path
      path = server.getConfiguration().getStoragePath(dbName);

      if (path == null)
        path = storageType + ":" + server.getDatabaseDirectory() + "/" + dbName;
    } else if (storageType.equals(OEngineMemory.NAME)) {
      path = storageType + ":" + dbName;
    } else
      throw new IllegalArgumentException("Cannot create database: storage mode '" + storageType + "' is not supported.");

    return Orient.instance().getDatabaseFactory().createDatabase(dbType, path);
  }

  protected int deleteRecord(final ODatabaseRecord iDatabase, final ORID rid, final ORecordVersion version) {
    try {
      // TRY TO SEE IF THE RECORD EXISTS
      final ORecord<?> record = rid.getRecord();
      if (record == null)
        return 0;

      iDatabase.delete(rid, version);
      return 1;
    } catch (Exception e) {
      return 0;
    }
  }

  protected int hideRecord(final ODatabaseRecord iDatabase, final ORID rid) {
    try {
      iDatabase.hide(rid);
      return 1;
    } catch (ORecordNotFoundException e) {
      return 0;
    }
  }

  protected int cleanOutRecord(final ODatabaseRecord iDatabase, final ORID rid, final ORecordVersion version) {
    iDatabase.delete(rid, version);
    return 1;
  }

  protected ORecordInternal<?> createRecord(final ODatabaseRecord iDatabase, final ORecordId rid, final byte[] buffer,
      final byte recordType, final int dataSegmentId) {
    final ORecordInternal<?> record = Orient.instance().getRecordFactoryManager().newInstance(recordType);
    record.fill(rid, OVersionFactory.instance().createVersion(), buffer, true);
    iDatabase.save(record);
    return record;
  }

  protected ORecordVersion updateRecord(final ODatabaseRecord iDatabase, final ORecordId rid, final byte[] buffer,
      final ORecordVersion version, final byte recordType) {
    final ORecordInternal<?> newRecord = Orient.instance().getRecordFactoryManager().newInstance(recordType);
    newRecord.fill(rid, version, buffer, true);

    final ORecordInternal<?> currentRecord;
    if (newRecord instanceof ODocument) {
      currentRecord = iDatabase.load(rid);

      if (currentRecord == null)
        throw new ORecordNotFoundException(rid.toString());

      ((ODocument) currentRecord).merge((ODocument) newRecord, false, false);

    } else
      currentRecord = newRecord;

    currentRecord.getRecordVersion().copyFrom(version);

    iDatabase.save(currentRecord);

    if (currentRecord.getIdentity().toString().equals(iDatabase.getStorage().getConfiguration().indexMgrRecordId)
        && !iDatabase.getStatus().equals(STATUS.IMPORTING)) {
      // FORCE INDEX MANAGER UPDATE. THIS HAPPENS FOR DIRECT CHANGES FROM REMOTE LIKE IN GRAPH
      iDatabase.getMetadata().getIndexManager().reload();
    }
    return currentRecord.getRecordVersion();
  }

  protected void handleConnectionError(final OChannelBinaryServer channel, final Throwable e) {
    try {
      channel.flush();
    } catch (IOException e1) {
    }
  }

  private void writeRecord(final ORecordInternal<?> iRecord) throws IOException {
    channel.writeShort((short) 0);
    channel.writeByte(iRecord.getRecordType());
    channel.writeRID(iRecord.getIdentity());
    channel.writeVersion(iRecord.getRecordVersion());
    try {
      final byte[] stream = iRecord.toStream();
      int realLength = stream.length;
      // TODO: This Logic should not be here provide an api in the Serializer for ask for trimmed content.
      final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
      if (db != null && db instanceof ODatabaseDocument) {
        if (db.getSerializer() instanceof ORecordSerializerSchemaAware2CSV) {
          // TRIM TAILING SPACES (DUE TO OVERSIZE)
          for (int i = stream.length - 1; i > -1; --i) {
            if (stream[i] == 32)
              --realLength;
            else
              break;
          }

        }
      }

      channel.writeBytes(stream, realLength);
    } catch (Exception e) {
      channel.writeBytes(null);
      final String message = "Error on unmarshalling record " + iRecord.getIdentity().toString() + " (" + e + ")";
      OLogManager.instance().error(this, message, e);

      throw new OSerializationException(message, e);
    }
  }
}
