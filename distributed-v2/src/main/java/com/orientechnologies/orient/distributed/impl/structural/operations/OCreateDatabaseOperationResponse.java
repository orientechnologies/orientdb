package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OCreateDatabaseOperationResponse implements OStructuralNodeResponse {
  private boolean created;

  public OCreateDatabaseOperationResponse(boolean created) {
    this.created = created;
  }

  public OCreateDatabaseOperationResponse() {

  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeBoolean(created);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    this.created = input.readBoolean();
  }

  @Override
  public int getResponseType() {
    return OCoordinateMessagesFactory.CREATE_DATABASE_RESPONSE;
  }

  public boolean isCreated() {
    return created;
  }
}
