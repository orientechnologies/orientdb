package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.Collection;
import java.util.Set;

public interface ODistributedResponseManagerFactory {

  ODistributedResponseManager newResponseManager(
      ODistributedRequest iRequest,
      Collection<ONodeId> iNodes,
      ORemoteTask task,
      Set<ONodeId> nodesConcurToTheQuorum,
      int availableNodes,
      int expectedResponses,
      int quorum,
      boolean groupByResponse,
      boolean waitLocalNode);
}
