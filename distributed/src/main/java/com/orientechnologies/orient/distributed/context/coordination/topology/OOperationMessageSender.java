package com.orientechnologies.orient.distributed.context.coordination.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import java.util.Set;

public interface OOperationMessageSender {

  void send(OOperationMessage message);

  void establish(Set<ONodeId> nodes, OOperationMessage message);
}
