package com.orientechnologies.orient.distributed;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;
import com.orientechnologies.orient.distributed.impl.structural.OStructuralSubmitRequest;
import com.orientechnologies.orient.distributed.impl.structural.raft.OLeaderContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class SyncRequest implements OStructuralSubmitRequest {
  private Optional<OLogId> logId;

  public SyncRequest(Optional<OLogId> logId) {
    this.logId = logId;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    if (logId.isPresent()) {
      output.writeBoolean(true);
      OLogId.serialize(logId.get(), output);
    } else {
      output.writeBoolean(false);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    boolean isPresent = input.readBoolean();
    if (isPresent) {
      logId = Optional.of(OLogId.deserialize(input));
    } else {
      logId = Optional.empty();
    }
  }

  @Override
  public int getRequestType() {
    return 0;
  }

  @Override
  public void begin(Optional<ONodeIdentity> requester, OSessionOperationId id, OLeaderContext context) {
    if (this.logId.isPresent()) {
      context.tryResend(requester.get(),this.logId.get());
    } else {
      context.sendFullConfiguration(requester.get());
    }
  }
}
