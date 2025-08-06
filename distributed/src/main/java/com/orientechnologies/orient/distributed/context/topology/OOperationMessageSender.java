package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.db.OOperationMessage;
import java.util.Set;

public interface OOperationMessageSender {

  void send(OOperationMessage message);

  void enstablish(Set<ONodeId> nodes, OOperationMessage message);
}
