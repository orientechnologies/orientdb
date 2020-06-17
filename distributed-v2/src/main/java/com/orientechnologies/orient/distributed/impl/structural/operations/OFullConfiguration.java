package com.orientechnologies.orient.distributed.impl.structural.operations;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.STRUCTURAL_FULL_CONFIGURATION;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import com.orientechnologies.orient.distributed.impl.structural.OReadStructuralSharedConfiguration;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OFullConfiguration implements OOperation {
  private OLogId lastId;
  private OReadStructuralSharedConfiguration shared;

  public OFullConfiguration(OLogId lastId, OReadStructuralSharedConfiguration shared) {
    this.lastId = lastId;
    this.shared = shared;
  }

  public OFullConfiguration() {}

  @Override
  public int getOperationId() {
    return STRUCTURAL_FULL_CONFIGURATION;
  }

  @Override
  public void apply(ONodeIdentity sender, OrientDBDistributed context) {
    context.syncToConfiguration(lastId, shared);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    shared.networkDeserialize(input);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    shared.networkSerialize(output);
  }
}
