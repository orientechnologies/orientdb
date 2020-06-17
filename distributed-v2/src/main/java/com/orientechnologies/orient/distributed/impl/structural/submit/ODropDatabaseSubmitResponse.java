package com.orientechnologies.orient.distributed.impl.structural.submit;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DROP_DATABASE_SUBMIT_RESPONSE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODropDatabaseSubmitResponse implements OStructuralSubmitResponse {
  private boolean success;
  private String error;

  public ODropDatabaseSubmitResponse(boolean success, String error) {
    this.success = success;
    this.error = error;
  }

  public ODropDatabaseSubmitResponse() {}

  public void serialize(DataOutput output) throws IOException {
    output.writeBoolean(success);
    output.writeBoolean(error != null);
    if (error != null) {
      output.writeUTF(error);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.success = input.readBoolean();
    if (input.readBoolean()) {
      this.error = input.readUTF();
    }
  }

  @Override
  public int getResponseType() {
    return DROP_DATABASE_SUBMIT_RESPONSE;
  }

  public String getError() {
    return error;
  }

  public boolean isSuccess() {
    return success;
  }
}
