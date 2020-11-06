package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionResultPayload;
import java.util.List;
import java.util.Optional;

public interface ODistributedTxResponseManager extends ODistributedResponseManager {
  boolean isQuorumReached();

  String getNodeNameFromPayload(OTransactionResultPayload payload);

  List<OTransactionResultPayload> getAllResponses();

  Optional<OTransactionResultPayload> getDistributedTxFinalResponse();

  boolean collectResponse(OTransactionPhase1TaskResult response, String senderNodeName);
}
