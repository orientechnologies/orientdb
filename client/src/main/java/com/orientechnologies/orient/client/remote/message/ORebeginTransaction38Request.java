package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.util.Map;

/** Created by tglman on 30/12/16. */
public class ORebeginTransaction38Request extends OBeginTransaction38Request {

  public ORebeginTransaction38Request(
      int txId,
      boolean usingLong,
      Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> changes) {
    super(txId, true, usingLong, operations, changes);
  }

  public ORebeginTransaction38Request() {}

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_REBEGIN;
  }

  @Override
  public String getDescription() {
    return "Re-begin transaction";
  }
}
