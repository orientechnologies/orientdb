package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.util.Map;
import java.util.Optional;

public interface OLeaderContext {
  boolean tryResend(ONodeIdentity identity, OLogId logId);

  void sendFullConfiguration(ONodeIdentity identity);

  void createDatabase(
      Optional<ONodeIdentity> requester,
      OSessionOperationId operationId,
      String database,
      String type,
      Map<String, String> configurations);

  void dropDatabase(Optional<ONodeIdentity> requester, OSessionOperationId id, String database);

  interface OpFinished {
    void finished();
  }

  void propagateAndApply(ORaftOperation operation, OpFinished finished);

  OrientDBDistributed getOrientDB();
}
