package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OOperation {
  void apply(ONodeIdentity sender, OrientDBDistributed context);

  void deserialize(DataInput input) throws IOException;

  void serialize(DataOutput output) throws IOException;

  int getOperationId();
}
