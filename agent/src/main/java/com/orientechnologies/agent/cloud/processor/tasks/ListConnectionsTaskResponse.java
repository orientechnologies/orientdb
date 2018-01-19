package com.orientechnologies.agent.cloud.processor.tasks;

import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ListConnectionsTaskResponse implements NodeOperationResponse {
  private String connections;

  public ListConnectionsTaskResponse() {
  }

  public ListConnectionsTaskResponse(String connections) {
    this.connections = connections;
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeUTF(this.connections);

  }

  @Override
  public void read(DataInput in) throws IOException {

    this.connections = in.readUTF();
  }

  public String getConnections() {
    return connections;
  }
}
