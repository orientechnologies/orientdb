package com.orientechnologies.orient.server.distributed.impl.coordinator;

import java.io.*;

public interface OSubmitResponse {
  void serialize(DataOutput output) throws IOException;

  void deserialize(DataInput input) throws IOException;
}
