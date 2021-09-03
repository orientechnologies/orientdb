package com.orientechnologies.orient.distributed.impl.structural.submit;

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OCreateDatabaseSubmitResponse implements OStructuralSubmitResponse {

  private boolean success;
  private String error;

  public OCreateDatabaseSubmitResponse() {}

  public OCreateDatabaseSubmitResponse(boolean success, String error) {
    this.success = success;
    this.error = error;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeBoolean(success);
    output.writeUTF(error);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.success = input.readBoolean();
    this.error = input.readUTF();
  }

  @Override
  public int getResponseType() {
    return OCoordinateMessagesFactory.CREATE_DATABASE_SUBMIT_RESPONSE;
  }

  public boolean isSuccess() {
    return success;
  }

  public String getError() {
    return error;
  }
}
