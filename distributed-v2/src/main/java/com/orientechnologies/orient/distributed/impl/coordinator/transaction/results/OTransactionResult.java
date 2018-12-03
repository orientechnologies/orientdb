package com.orientechnologies.orient.distributed.impl.coordinator.transaction.results;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OTransactionResult {

  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;
}
