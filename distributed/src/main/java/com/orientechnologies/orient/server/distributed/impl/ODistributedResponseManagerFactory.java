package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.ODistributedResponseManager;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;

import java.util.Collection;
import java.util.Set;

public interface ODistributedResponseManagerFactory {

  ODistributedResponseManager newResponseManager(ODistributedRequest iRequest, Collection<String> iNodes,
      OCallable<Void, ODistributedResponseManager> endCallback, ORemoteTask task, Set<String> nodesConcurToTheQuorum,
      int availableNodes, int expectedResponses, int quorum, boolean groupByResponse, boolean waitLocalNode);

}
