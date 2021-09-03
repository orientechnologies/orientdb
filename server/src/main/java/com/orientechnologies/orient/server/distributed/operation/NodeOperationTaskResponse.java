package com.orientechnologies.orient.server.distributed.operation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NodeOperationTaskResponse implements Externalizable {
  private int messageId;
  private NodeOperationResponse response;

  public NodeOperationTaskResponse() {}

  public NodeOperationTaskResponse(int messageId, NodeOperationResponse response) {
    this.messageId = messageId;
    this.response = response;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(this.messageId);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(outputStream);
    response.write(stream);
    byte[] bytes = outputStream.toByteArray();
    out.writeInt(bytes.length);
    out.write(outputStream.toByteArray());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.messageId = in.readInt();
    int size = in.readInt();
    byte[] message = new byte[size];
    in.readFully(message, 0, size);
    this.response = NodeOperationTask.createOperationResponse(messageId);
    this.response.read(new DataInputStream(new ByteArrayInputStream(message)));
  }

  public NodeOperationResponse getResponse() {
    return response;
  }
}
