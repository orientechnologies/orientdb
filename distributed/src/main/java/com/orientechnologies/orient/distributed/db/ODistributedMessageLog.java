package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.ODistributedMessage;
import java.util.List;

public interface ODistributedMessageLog {

  void log(ODistributedMessage message);

  List<ODistributedMessage> recover(List<OTransactionId> ids);
}
