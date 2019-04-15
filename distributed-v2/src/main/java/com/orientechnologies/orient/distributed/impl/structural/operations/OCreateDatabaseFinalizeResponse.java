package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralNodeResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OCreateDatabaseFinalizeResponse implements OStructuralNodeResponse {
  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getResponseType() {
    return OCoordinateMessagesFactory.CREATE_DATABASE_FINALIZE_RESPONSE;
  }
}
