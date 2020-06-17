package com.orientechnologies.orient.distributed.impl.coordinator.mocktx;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.ORequestContext;
import com.orientechnologies.orient.distributed.impl.coordinator.OResponseHandler;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondPhaseResponseHandler implements OResponseHandler {
  private final OSubmitTx submitTx;
  private final ONodeIdentity member;
  boolean done = false;

  public SecondPhaseResponseHandler(OSubmitTx submitTx, ONodeIdentity member) {
    this.member = member;
    this.submitTx = submitTx;
  }

  @Override
  public boolean receive(
      ODistributedCoordinator coordinator,
      ORequestContext context1,
      ONodeIdentity member,
      ONodeResponse response) {
    if (context1.getResponses().size() >= context1.getQuorum() && !done) {
      done = true;
      submitTx.secondPhase = true;
      coordinator.reply(
          this.member,
          new OSessionOperationId(),
          new OSubmitResponse() {
            @Override
            public void serialize(DataOutput output) throws IOException {}

            @Override
            public void deserialize(DataInput input) throws IOException {}

            @Override
            public int getResponseType() {
              return 0;
            }
          });
    }
    return context1.getResponses().size() == context1.getInvolvedMembers().size();
  }

  @Override
  public boolean timeout(ODistributedCoordinator coordinator, ORequestContext context) {
    return true;
  }
}
