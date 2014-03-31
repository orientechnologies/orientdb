package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Asynchronous command result manager. As soon as a record is returned by the command is sent over the wire.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OAsyncCommandResultListener extends OAbstractCommandResultListener {

  private final ONetworkProtocolBinary protocol;
  private final AtomicBoolean          empty = new AtomicBoolean(true);
  private final int                    txId;

  public OAsyncCommandResultListener(final ONetworkProtocolBinary iNetworkProtocolBinary, final int txId) {
    this.protocol = iNetworkProtocolBinary;
    this.txId = txId;
  }

  @Override
  public boolean result(final Object iRecord) {
    if (empty.compareAndSet(true, false))
      try {
        protocol.sendOk(txId);
      } catch (IOException e1) {
      }

    try {
      fetchRecord(iRecord, new ORemoteFetchListener() {
        @Override
        protected void sendRecord(ORecord<?> iLinked) {
          try {
            if (protocol.connection.data.protocolVersion >= 17) {
              protocol.channel.writeByte((byte) 2); // CACHE IT ON THE CLIENT
              protocol.writeIdentifiable(iLinked);
            }
          } catch (IOException e) {
            OLogManager.instance().error(this, "Cannot write against channel", e);
          }
        }
      });

      protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
      protocol.writeIdentifiable((ORecordInternal<?>) ((OIdentifiable) iRecord).getRecord());

    } catch (IOException e) {
      return false;
    }

    return true;
  }

  public boolean isEmpty() {
    return empty.get();
  }
}
