package com.orientechnologies.agent.cloud.processor.tasks;

import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EnterpriseStatsResponse implements NodeOperationResponse {
  private String stats;

  public EnterpriseStatsResponse() {
  }

  public EnterpriseStatsResponse(String stats) {
    this.stats = stats;
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeUTF(this.stats);

  }

  @Override
  public void read(DataInput in) throws IOException {

    this.stats = in.readUTF();
  }

  public String getStats() {
    return stats;
  }
}
