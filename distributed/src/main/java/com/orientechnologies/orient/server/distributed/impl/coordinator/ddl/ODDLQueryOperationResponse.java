package com.orientechnologies.orient.server.distributed.impl.coordinator.ddl;

import com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeResponse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODDLQueryOperationResponse implements ONodeResponse {
  @Override
  public void serialize(DataOutput output) throws IOException {

  }

  @Override
  public void deserialize(DataInput input) throws IOException {

  }

  @Override
  public int getResponseType() {
    return OCoordinateMessagesFactory.DDL_QUERY_NODE_RESPONSE;
  }
}
