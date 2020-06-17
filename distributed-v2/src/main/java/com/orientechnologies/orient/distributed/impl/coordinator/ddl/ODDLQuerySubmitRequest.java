package com.orientechnologies.orient.distributed.impl.coordinator.ddl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODDLQuerySubmitRequest implements OSubmitRequest {

  private String query;

  public ODDLQuerySubmitRequest(String query) {
    this.query = query;
  }

  public ODDLQuerySubmitRequest() {}

  public String getQuery() {
    return query;
  }

  @Override
  public void begin(
      ONodeIdentity requester,
      OSessionOperationId operationId,
      ODistributedCoordinator coordinator) {
    coordinator.sendOperation(
        this,
        new ODDLQueryOperationRequest(query),
        new ODDLQueryResultHandler(requester, operationId));
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeUTF(query);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    query = input.readUTF();
  }

  @Override
  public int getRequestType() {
    return OCoordinateMessagesFactory.DDL_QUERY_SUBMIT_REQUEST;
  }
}
