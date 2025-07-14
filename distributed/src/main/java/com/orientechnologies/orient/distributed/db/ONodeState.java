package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.tx.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderImpl;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.server.distributed.ODistributedMessage;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionSequenceManager;
import java.util.List;
import java.util.Optional;

public class ONodeState {

  private OTransactionSequenceManager sequenceManager;
  private ODistributedMessageLog log;
  private OPromisedDistributedOps promised;
  private OCoordinatedDistributedOps coordinated;

  public ONodeState(ONodeId coordinator) {
    sequenceManager = new OTransactionSequenceManager(coordinator, 3);
    log = new ODistributedMessageLogMemory();
    promised = new OPromisedDistributedOpsImpl();
    coordinated = new OCoordinatedDistributedOpsImpl();
  }

  private void fill(Optional<byte[]> lastMetadata) {
    lastMetadata.ifPresent(
        (data) -> sequenceManager.fill(OTxMetadataHolderImpl.read(data).getStatus()));
  }

  public boolean receive(ODistributedMessage message) {
    ValidationResult result = sequenceManager.validate(message.getPromiseId());
    switch (result) {
      case VALID -> {
        this.log.log(message);
        this.promised.add(message);
        return true;
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        return false;
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else
        return false;
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one
        return false;
      }
    }
    return false;
  }

  public void receiveFailure(OTransactionIdPromise promise) {
    boolean promised = sequenceManager.notifyFailure(promise);
    if (promised) {
      this.promised.remove(promise);
    }
  }

  public ODistributedMessage receiveSuccess(OTransactionIdPromise promise) {
    // TODO: the verification of success also close the promise, maybe is better to close
    // the promise after the execution;
    ValidationResult result = sequenceManager.notifySuccess(promise);
    switch (result) {
      case VALID -> {
        ODistributedMessage message = this.promised.get(promise);
        return message;
      }
      case ALREADY_PRESENT -> {
        // Already present ... maybe do nothing, already done
        this.promised.remove(promise);
      }
      case ALREADY_PROMISED -> {
        // Fail for promised to someone else
      }
      case MISSING_PREVIOUS -> {
        // wait for previous one
      }
    }

    return null;
  }

  public void complete(OTransactionIdPromise promise) {
    this.promised.remove(promise);
  }

  public List<ODistributedMessage> recover(List<OTransactionId> ids) {
    return this.log.recover(ids);
  }
}
