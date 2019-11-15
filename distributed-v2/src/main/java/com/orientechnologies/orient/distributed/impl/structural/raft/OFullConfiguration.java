package com.orientechnologies.orient.distributed.impl.structural.raft;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.OReadStructuralSharedConfiguration;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSharedConfiguration;

public class OFullConfiguration implements OOperation {
  private OLogId                             lastId;
  private OReadStructuralSharedConfiguration shared;

  public OFullConfiguration(OLogId lastId, OReadStructuralSharedConfiguration shared) {
    this.lastId = lastId;
    this.shared = shared;
  }

  @Override
  public void apply(OrientDBDistributed context) {
    context.syncToConfiguration(lastId, shared);
  }
}
