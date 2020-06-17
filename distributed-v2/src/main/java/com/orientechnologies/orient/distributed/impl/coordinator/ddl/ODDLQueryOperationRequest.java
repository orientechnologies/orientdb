package com.orientechnologies.orient.distributed.impl.coordinator.ddl;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.DDL_QUERY_NODE_REQUEST;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ODDLQueryOperationRequest implements ONodeRequest {
  private String query;

  public ODDLQueryOperationRequest(String query) {
    this.query = query;
  }

  public ODDLQueryOperationRequest() {}

  @Override
  public ONodeResponse execute(
      ONodeIdentity nodeFrom,
      OLogId opId,
      ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          return session.command(query);
        });

    return new ODDLQueryOperationResponse();
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
    return DDL_QUERY_NODE_REQUEST;
  }

  public String getQuery() {
    return query;
  }
}
