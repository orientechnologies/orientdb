package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.*;

import java.util.Map;

public interface OMasterContext {
  void tryResend(ONodeIdentity identity, OLogId logId);

  void sendFullConfiguration(ONodeIdentity identity);

  void createDatabase(String database, String type, Map<String, String> configurations);

  interface OpFinished {
    void finished();
  }

  void propagateAndApply(ORaftOperation operation, OpFinished finished);

  OrientDBDistributed getOrientDB();

}
