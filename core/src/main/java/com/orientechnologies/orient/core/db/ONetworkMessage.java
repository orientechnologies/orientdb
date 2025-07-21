package com.orientechnologies.orient.core.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ONetworkMessage {

  void execute();

  void deserialize(DataInput input) throws IOException;

  void serialize(DataOutput out) throws IOException;
}
