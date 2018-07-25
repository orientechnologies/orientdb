package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.OCallable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface ODistributedResponseManager {
  boolean setLocalResult(String localNodeName, Object localResult);

  default Object getGenericFinalResponse() {
    return getFinalResponse();
  }

  ODistributedResponse getFinalResponse();

  void removeServerBecauseUnreachable(String node);

  boolean waitForSynchronousResponses() throws InterruptedException;

  long getSynchTimeout();

  void cancel();

  Set<String> getExpectedNodes();

  List<String> getRespondingNodes();

  Set<String> getServersWithoutFollowup();

  ODistributedRequestId getMessageId();

  ODistributedRequest getRequest();

  int getQuorum();

  boolean collectResponse(ODistributedResponse response);

  void timeout();

  long getSentOn();

  List<String> getMissingNodes();

  String getDatabaseName();

  boolean isFinished();
}
