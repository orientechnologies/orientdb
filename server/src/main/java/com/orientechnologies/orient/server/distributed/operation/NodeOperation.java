package com.orientechnologies.orient.server.distributed.operation;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.DataInput;
import java.io.DataOutput;

public interface NodeOperation {

  NodeOperationResponse execute(OServer iServer, ODistributedServerManager iManager);

  void write(DataOutput out);

  void read(DataInput in);

  int getMessageId();
}
