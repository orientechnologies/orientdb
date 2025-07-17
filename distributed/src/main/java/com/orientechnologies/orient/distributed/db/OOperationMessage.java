package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public interface OOperationMessage {

  Optional<OAcceptResult> validate(OrientDBDistributed ctx);

  void apply(OrientDBInternal ctx);

  static OOperationMessage readNetwork(DataInput input) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  void writeNetwork(DataOutput out) throws IOException;
}
