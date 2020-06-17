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

package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientSessions;
import com.orientechnologies.orient.server.OServer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous command result manager. As soon as a record is returned by the command is sent over
 * the wire.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OLiveCommandResultListener extends OAbstractCommandResultListener
    implements OLiveResultListener {

  private OClientConnection connection;
  private final AtomicBoolean empty = new AtomicBoolean(true);
  private final int sessionId;
  private final Set<ORID> alreadySent = new HashSet<ORID>();
  private OClientSessions session;

  public OLiveCommandResultListener(
      OServer server,
      final OClientConnection connection,
      OCommandResultListener wrappedResultListener) {
    super(wrappedResultListener);
    this.connection = connection;
    session = server.getClientConnectionManager().getSession(connection);
    this.sessionId = connection.getId();
  }

  @Override
  public boolean result(final Object iRecord) {
    final ONetworkProtocolBinary protocol = ((ONetworkProtocolBinary) connection.getProtocol());
    if (empty.compareAndSet(true, false))
      try {
        protocol.channel.writeByte(OChannelBinaryProtocol.RESPONSE_STATUS_OK);
        protocol.channel.writeInt(protocol.clientTxId);
        protocol.okSent = true;
        if (connection != null
            && Boolean.TRUE.equals(connection.getTokenBased())
            && connection.getToken() != null
            && protocol.requestType != OChannelBinaryProtocol.REQUEST_CONNECT
            && protocol.requestType != OChannelBinaryProtocol.REQUEST_DB_OPEN) {
          // TODO: Check if the token is expiring and if it is send a new token
          byte[] renewedToken =
              protocol.getServer().getTokenHandler().renewIfNeeded(connection.getToken());
          protocol.channel.writeBytes(renewedToken);
        }
      } catch (IOException ignored) {
      }
    try {
      fetchRecord(
          iRecord,
          new ORemoteFetchListener() {
            @Override
            protected void sendRecord(ORecord iLinked) {
              if (!alreadySent.contains(iLinked.getIdentity())) {
                alreadySent.add(iLinked.getIdentity());
                try {
                  protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
                  protocol.writeIdentifiable(protocol.channel, connection, iLinked);
                } catch (IOException e) {
                  OLogManager.instance().error(this, "Cannot write against channel", e);
                }
              }
            }
          });
      alreadySent.add(((OIdentifiable) iRecord).getIdentity());
      protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
      protocol.writeIdentifiable(
          protocol.channel, connection, ((OIdentifiable) iRecord).getRecord());
      protocol.channel.flush();
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  public boolean isEmpty() {
    return empty.get();
  }

  public void onLiveResult(int iToken, ORecordOperation iOp) throws OException {
    boolean sendFail = true;
    do {
      List<OClientConnection> connections = session.getConnections();
      if (connections.size() == 0) {
        try {
          ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
          OLogManager.instance()
              .warn(this, "Unsubscribing live query for connection " + connection);
          OLiveQueryHook.unsubscribe(iToken, db);
        } catch (Exception e) {
          OLogManager.instance()
              .warn(this, "Unsubscribing live query for connection " + connection, e);
        }
        break;
      }
      OClientConnection curConnection = connections.get(0);
      ONetworkProtocolBinary protocol = (ONetworkProtocolBinary) curConnection.getProtocol();

      OChannelBinary channel = protocol.getChannel();
      try {
        channel.acquireWriteLock();
        try {

          ByteArrayOutputStream content = new ByteArrayOutputStream();

          DataOutputStream out = new DataOutputStream(content);
          out.writeByte('r');
          out.writeByte(iOp.type);
          out.writeInt(iToken);
          out.writeByte(ORecordInternal.getRecordType(iOp.getRecord()));
          writeVersion(out, iOp.getRecord().getVersion());
          writeRID(out, (ORecordId) iOp.getRecord().getIdentity());
          writeBytes(out, protocol.getRecordBytes(connection, iOp.getRecord()));
          channel.writeByte(OChannelBinaryProtocol.PUSH_DATA);
          channel.writeInt(Integer.MIN_VALUE);
          channel.writeByte(OChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY);
          channel.writeBytes(content.toByteArray());
          channel.flush();

        } finally {
          channel.releaseWriteLock();
        }
        sendFail = false;
      } catch (IOException e) {
        session.removeConnection(curConnection);
        connections = session.getConnections();
        if (connections.isEmpty()) {
          ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
          OLiveQueryHook.unsubscribe(iToken, db);
          break;
        }
      } catch (Exception e) {
        OLogManager.instance()
            .warn(
                this,
                "Cannot push cluster configuration to the client %s",
                e,
                protocol.getRemoteAddress());
        protocol.getServer().getClientConnectionManager().disconnect(connection);
        OLiveQueryHook.unsubscribe(iToken, connection.getDatabase());
        break;
      }

    } while (sendFail);
  }

  @Override
  public void onError(int iLiveToken) {}

  @Override
  public void onUnsubscribe(int iLiveToken) {
    boolean sendFail = true;
    do {
      List<OClientConnection> connections = session.getConnections();
      if (connections.size() == 0) {
        break;
      }
      ONetworkProtocolBinary protocol = (ONetworkProtocolBinary) connections.get(0).getProtocol();

      OChannelBinary channel = protocol.getChannel();
      try {
        channel.acquireWriteLock();
        try {

          ByteArrayOutputStream content = new ByteArrayOutputStream();

          DataOutputStream out = new DataOutputStream(content);
          out.writeByte('u');
          out.writeInt(iLiveToken);
          channel.writeByte(OChannelBinaryProtocol.PUSH_DATA);
          channel.writeInt(Integer.MIN_VALUE);
          channel.writeByte(OChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY);
          channel.writeBytes(content.toByteArray());
          channel.flush();

        } finally {
          channel.releaseWriteLock();
        }
        sendFail = false;
      } catch (IOException e) {
        connections = session.getConnections();
        if (connections.isEmpty()) {
          break;
        }
      } catch (Exception e) {
        OLogManager.instance()
            .warn(
                this,
                "Cannot push cluster configuration to the client %s",
                e,
                protocol.getRemoteAddress());
        protocol.getServer().getClientConnectionManager().disconnect(connection);
        break;
      }

    } while (sendFail);
  }

  private void writeVersion(DataOutputStream out, int v) throws IOException {
    out.writeInt(v);
  }

  private void writeRID(DataOutputStream out, ORecordId record) throws IOException {
    out.writeShort((short) record.getClusterId());
    out.writeLong(record.getClusterPosition());
  }

  public void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public void linkdedBySimpleValue(ODocument doc) {}
}
