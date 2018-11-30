package com.orientechnologies.orient.distributed.impl.coordinator.ddl;

import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DDL_QUERY_SUBMIT_RESPONSE;

public class ODDLQuerySubmitResponse implements OSubmitResponse {
  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getResponseType() {
    return DDL_QUERY_SUBMIT_RESPONSE;
  }
}
