package com.orientechnologies.orient.server.distributed;

import java.util.List;
import java.util.Set;

public interface ODistributedResponseManager {
  boolean setLocalResult(String localNodeName, Object localResult);

  ODistributedResponse getFinalResponse();

  void removeServerBecauseUnreachable(String node);

  boolean waitForSynchronousResponses() throws InterruptedException;

  long getSynchTimeout();

  void cancel();

  Set<String> getExpectedNodes();

  List<String> getRespondingNodes();

  ODistributedRequestId getMessageId();

  int getQuorum();

  boolean collectResponse(ODistributedResponse response);

  void timeout();

  long getSentOn();

  List<String> getMissingNodes();

  String getDatabaseName();

  boolean isFinished();
}
