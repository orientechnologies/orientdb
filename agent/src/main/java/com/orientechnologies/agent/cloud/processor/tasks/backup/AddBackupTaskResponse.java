package com.orientechnologies.agent.cloud.processor.tasks.backup;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.operation.NodeOperationResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Enrico Risa on 17/01/2018.
 */
public class AddBackupTaskResponse implements NodeOperationResponse {

  private ODocument config;

  public AddBackupTaskResponse() {
  }

  public AddBackupTaskResponse(ODocument config) {
    this.config = config;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(config.toJSON());
  }

  @Override
  public void read(DataInput in) throws IOException {
    config = new ODocument().fromJSON(in.readUTF());
  }

  public ODocument getConfig() {
    return config;
  }
}
