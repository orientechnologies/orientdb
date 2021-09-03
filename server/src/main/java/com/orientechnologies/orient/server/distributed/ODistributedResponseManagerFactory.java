package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.Collection;
import java.util.Set;

public interface ODistributedResponseManagerFactory {

  ODistributedResponseManager newResponseManager(
      ODistributedRequest iRequest,
      Collection<String> iNodes,
      ORemoteTask task,
      Set<String> nodesConcurToTheQuorum,
      int availableNodes,
      int expectedResponses,
      int quorum,
      boolean groupByResponse,
      boolean waitLocalNode);
}
