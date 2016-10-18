/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.ORemoteConnectionPool;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OBasicResultSet;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public final class OCommandResponse implements OBinaryResponse<Object> {
  private final OStorageRemote      storage;
  private final boolean             asynch;
  private final OCommandRequestText iCommand;
  private final ODatabaseDocument   database;
  private final boolean             live;

  public OCommandResponse(OStorageRemote storage, boolean asynch, OCommandRequestText iCommand, ODatabaseDocument database,
      boolean live) {
    this.storage = storage;
    this.asynch = asynch;
    this.iCommand = iCommand;
    this.database = database;
    this.live = live;
  }

  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    throw new UnsupportedOperationException();

  }

  @Override
  public Object read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    Object result = null;
    try {
      // Collection of prefetched temporary record (nested projection record), to refer for avoid garbage collection.
      List<ORecord> temporaryResults = new ArrayList<ORecord>();

      boolean addNextRecord = true;
      if (asynch) {
        byte status;

        // ASYNCH: READ ONE RECORD AT TIME
        while ((status = network.readByte()) > 0) {
          final ORecord record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);
          if (record == null)
            continue;

          switch (status) {
          case 1:
            // PUT AS PART OF THE RESULT SET. INVOKE THE LISTENER
            if (addNextRecord) {
              addNextRecord = iCommand.getResultListener().result(record);
              database.getLocalCache().updateRecord(record);
            }
            break;

          case 2:
            if (record.getIdentity().getClusterId() == -2)
              temporaryResults.add(record);
            // PUT IN THE CLIENT LOCAL CACHE
            database.getLocalCache().updateRecord(record);
          }
        }
      } else {
        result = readSynchResult(network, database, temporaryResults);
        if (live) {
          final ODocument doc = ((List<ODocument>) result).get(0);
          final Integer token = doc.field("token");
          final Boolean unsubscribe = doc.field("unsubscribe");
          if (token != null) {
            if (Boolean.TRUE.equals(unsubscribe)) {
              if (this.storage.asynchEventListener != null)
                this.storage.asynchEventListener.unregisterLiveListener(token);
            } else {
              final OLiveResultListener listener = (OLiveResultListener) iCommand.getResultListener();
              ODatabaseDocumentInternal current = ODatabaseRecordThreadLocal.INSTANCE.get();
              final ODatabaseDocument dbCopy = current.copy();
              ORemoteConnectionPool pool = this.storage.connectionManager.getPool(network.getServerURL());
              this.storage.asynchEventListener.registerLiveListener(pool, token, new OLiveResultListener() {

                @Override
                public void onUnsubscribe(int iLiveToken) {
                  listener.onUnsubscribe(iLiveToken);
                  dbCopy.close();
                }

                @Override
                public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
                  dbCopy.activateOnCurrentThread();
                  listener.onLiveResult(iLiveToken, iOp);
                }

                @Override
                public void onError(int iLiveToken) {
                  listener.onError(iLiveToken);
                  dbCopy.close();
                }
              });
            }
          } else {
            throw new OStorageException("Cannot execute live query, returned null token");
          }
        }
      }
      if (!temporaryResults.isEmpty()) {
        if (result instanceof OBasicResultSet<?>) {
          ((OBasicResultSet<?>) result).setTemporaryRecordCache(temporaryResults);
        }
      }
    } finally {
      // TODO: this is here because we allow query in end listener.
      session.commandExecuting = false;
      if (iCommand.getResultListener() != null && !live)
        iCommand.getResultListener().end();
    }
    return result;
  }

  protected Object readSynchResult(final OChannelBinaryAsynchClient network, final ODatabaseDocument database,
      List<ORecord> temporaryResults) throws IOException {

    final Object result;

    final byte type = network.readByte();
    switch (type) {
    case 'n':
      result = null;
      break;

    case 'r':
      result = OChannelBinaryProtocol.readIdentifiable(network);
      if (result instanceof ORecord)
        database.getLocalCache().updateRecord((ORecord) result);
      break;

    case 'l':
    case 's':
      final int tot = network.readInt();
      final Collection<OIdentifiable> coll;

      coll = type == 's' ? new HashSet<OIdentifiable>(tot) : new OBasicResultSet<OIdentifiable>(tot);
      for (int i = 0; i < tot; ++i) {
        final OIdentifiable resultItem = OChannelBinaryProtocol.readIdentifiable(network);
        if (resultItem instanceof ORecord)
          database.getLocalCache().updateRecord((ORecord) resultItem);
        coll.add(resultItem);
      }

      result = coll;
      break;
    case 'i':
      coll = new OBasicResultSet<OIdentifiable>();
      byte status;
      while ((status = network.readByte()) > 0) {
        final OIdentifiable record = OChannelBinaryProtocol.readIdentifiable(network);
        if (record == null)
          continue;
        if (status == 1) {
          if (record instanceof ORecord)
            database.getLocalCache().updateRecord((ORecord) record);
          coll.add(record);
        }
      }
      result = coll;
      break;
    case 'w':
      final OIdentifiable record = OChannelBinaryProtocol.readIdentifiable(network);
      // ((ODocument) record).setLazyLoad(false);
      result = ((ODocument) record).field("result");
      break;

    default:
      OLogManager.instance().warn(this, "Received unexpected result from query: %d", type);
      result = null;
    }

    if (network.getSrvProtocolVersion() >= 17) {
      // LOAD THE FETCHED RECORDS IN CACHE
      byte status;
      while ((status = network.readByte()) > 0) {
        final ORecord record = (ORecord) OChannelBinaryProtocol.readIdentifiable(network);
        if (record != null && status == 2) {
          // PUT IN THE CLIENT LOCAL CACHE
          database.getLocalCache().updateRecord(record);
          if (record.getIdentity().getClusterId() == -2)
            temporaryResults.add(record);
        }
      }
    }

    return result;
  }
}