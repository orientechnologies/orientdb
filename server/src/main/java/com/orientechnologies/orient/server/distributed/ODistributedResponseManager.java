package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.transaction.ONodeId;
import java.util.List;
import java.util.Set;

public interface ODistributedResponseManager {
  boolean setLocalResult(ONodeId localNodeName, Object localResult);

  ODistributedResponse getFinalResponse();

  void removeServerBecauseUnreachable(ONodeId node);

  boolean waitForSynchronousResponses() throws InterruptedException;

  long getSynchTimeout();

  void cancel();

  Set<ONodeId> getExpectedNodes();

  List<ONodeId> getRespondingNodes();

  ODistributedRequestId getMessageId();

  int getQuorum();

  boolean collectResponse(ODistributedResponse response);

  void timeout();

  long getSentOn();

  List<ONodeId> getMissingNodes();

  String getDatabaseName();

  boolean isFinished();
}
