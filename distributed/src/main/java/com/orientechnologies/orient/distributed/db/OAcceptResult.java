package com.orientechnologies.orient.distributed.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface OAcceptResult {

  boolean canRetry();

  void writeNetwork(DataOutput out);

  static OAcceptResult readNetwork(DataInput input) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
