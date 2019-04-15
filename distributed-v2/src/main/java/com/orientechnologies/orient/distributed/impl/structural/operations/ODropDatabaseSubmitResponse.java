package com.orientechnologies.orient.distributed.impl.structural.operations;

import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DROP_DATABASE_SUBMIT_RESPONSE;

public class ODropDatabaseSubmitResponse implements OStructuralSubmitResponse {
  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getResponseType() {
    return DROP_DATABASE_SUBMIT_RESPONSE;
  }
}
