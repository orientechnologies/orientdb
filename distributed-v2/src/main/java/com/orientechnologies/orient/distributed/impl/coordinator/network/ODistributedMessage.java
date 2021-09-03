package com.orientechnologies.orient.distributed.impl.coordinator.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface ODistributedMessage {

  void write(final DataOutput output) throws IOException;

  void read(final DataInput input) throws IOException;

  byte getCommand();

  void execute(ONodeIdentity sender, OCoordinatedExecutor executor);
}
