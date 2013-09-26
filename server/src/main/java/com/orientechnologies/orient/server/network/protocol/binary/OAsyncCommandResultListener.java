package com.orientechnologies.orient.server.network.protocol.binary;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;

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
      protocol.channel.writeByte((byte) 1); // ONE MORE RECORD
      protocol.writeIdentifiable((ORecordInternal<?>) ((OIdentifiable) iRecord).getRecord());

      fetchRecord(iRecord);

    } catch (IOException e) {
      return false;
    }

    return true;
  }

  public boolean isEmpty() {
    return empty.get();
  }
}