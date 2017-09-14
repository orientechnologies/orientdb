package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.util.OCallable;

import java.util.Collection;
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

  Collection<String> getConflictServers();

  Set<String> getServersWithoutFollowup();

  boolean addFollowupToServer(String node);

  boolean isSynchronousWaiting();

  ODistributedResponse getQuorumResponse();

  ODistributedRequestId getMessageId();

  ODistributedRequest getRequest();

  Object getResponseFromServer(String server);

  int getQuorum();

  boolean executeInLock(OCallable<Boolean, ODistributedResponseManager> oCallable);

  boolean collectResponse(ODistributedResponse response);

  void timeout();

  long getSentOn();

  List<String> getMissingNodes();

  String getDatabaseName();
}
