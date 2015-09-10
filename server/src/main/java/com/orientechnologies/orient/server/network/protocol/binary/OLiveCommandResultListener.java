/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.server.network.protocol.binary;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryServer;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OClientSessions;

/**
 * Asynchronous command result manager. As soon as a record is returned by the command is sent over the wire.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 */
public class OLiveCommandResultListener extends OAbstractCommandResultListener implements OLiveResultListener {

  private ONetworkProtocolBinary protocol;
  private final AtomicBoolean    empty       = new AtomicBoolean(true);
  private final int              txId;
  private final Set<ORID>        alreadySent = new HashSet<ORID>();
  private OClientSessions        session;

  public OLiveCommandResultListener(final ONetworkProtocolBinary iNetworkProtocolBinary, final int txId,
      OCommandResultListener wrappedResultListener) {
    super(wrappedResultListener);
    this.protocol = iNetworkProtocolBinary;
    session = iNetworkProtocolBinary.getServer().getClientConnectionManager().getSession(iNetworkProtocolBinary);
    this.txId = txId;
  }

  @Override
  public boolean result(final Object iRecord) {
    if (empty.compareAndSet(true, false))
      try {
        protocol.sendOk(txId);
      } catch (IOException ignored) {
      }
    try {
      fetchRecord(iRecord, new ORemoteFetchListener() {
        @Override
        protected void sendRecord(ORecord iLinked) {
          if (!alreadySent.contains(iLinked.getIdentity())) {
            alreadySent.add(iLinked.getIdentity());
            try {
              protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
              protocol.writeIdentifiable(iLinked);
            } catch (IOException e) {
              OLogManager.instance().error(this, "Cannot write against channel", e);
            }
          }
        }
      });
      alreadySent.add(((OIdentifiable) iRecord).getIdentity());
      protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
      protocol.writeIdentifiable(((OIdentifiable) iRecord).getRecord());
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
      OChannelBinaryServer channel = protocol.channel;
      try {
        channel.acquireWriteLock();
        try {

          ByteArrayOutputStream content = new ByteArrayOutputStream();

          DataOutputStream out = new DataOutputStream(content);
          out.writeByte(iOp.type);
          out.writeInt(iToken);
          out.writeByte(ORecordInternal.getRecordType(iOp.getRecord()));
          writeVersion(out, iOp.getRecord().getRecordVersion());
          writeRID(out, (ORecordId) iOp.getRecord().getIdentity());
          writeBytes(out, protocol.getRecordBytes(iOp.getRecord()));

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
        List<OClientConnection> connections = session.getConnections();
        if (connections.isEmpty()) {
          OLiveQueryHook.unsubscribe(iToken, protocol.connection.database);
          break;
        }
        protocol = (ONetworkProtocolBinary) connections.get(0).getProtocol();
      } catch (Exception e) {
        OLogManager.instance().warn(this, "Cannot push cluster configuration to the client %s", e,
            protocol.connection.getRemoteAddress());
        protocol.getServer().getClientConnectionManager().disconnect(protocol.connection);
        OLiveQueryHook.unsubscribe(iToken, protocol.connection.database);
        break;
      }

    } while (sendFail);
  }

  @Override
  public void onError(int iLiveToken) {

  }

  @Override
  public void onUnsubscribe(int iLiveToken) {

  }

  private void writeVersion(DataOutputStream out, ORecordVersion v) throws IOException {
    final ORecordVersion version = OVersionFactory.instance().createVersion();
    out.writeInt(version.getCounter());
  }

  private void writeRID(DataOutputStream out, ORecordId record) throws IOException {
    out.writeShort((short) record.getClusterId());
    out.writeLong(record.getClusterPosition());
  }

  public void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }
}
