package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.ODistributedMessage;
import java.util.List;

public class ODistributedMessageLogMemory implements ODistributedMessageLog {

  @Override
  public void log(ODistributedMessage message) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<ODistributedMessage> recover(List<OTransactionId> ids) {
    // TODO Auto-generated method stub
    return null;
  }
}
