package com.orientechnologies.orient.server.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ODistributedMessage {

  void toStream(final DataOutput out) throws IOException;

  void fromStream(final DataInput in) throws IOException;
}
