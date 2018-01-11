package com.orientechnologies.orient.server.distributed.operation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NodeOperationTaskResponse implements Externalizable {
  private int                   messageId;
  private NodeOperationResponse response;

  public NodeOperationTaskResponse(int messageId, NodeOperationResponse response) {
    this.messageId = messageId;
    this.response = response;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.write(this.messageId);
    response.write(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.messageId = in.readInt();
    this.response = NodeOperationTask.createOperationResponse(messageId);
    this.response.read(in);
  }

  public NodeOperationResponse getResponse() {
    return response;
  }
}
