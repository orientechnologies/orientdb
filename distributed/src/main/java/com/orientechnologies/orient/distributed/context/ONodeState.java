package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.core.transaction.OTransactionSequenceManager;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderImpl;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.server.distributed.ODistributedMessage;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ONodeState {

  private OTransactionSequenceManager sequenceManager;
  private ODistributedMessageLog log;
  private OPromisedDistributedOps promised;
  private OCoordinatedDistributedOps coordinated;

  public ONodeState(ONodeId coordinator) {
    sequenceManager = new OTransactionSequenceManager(coordinator, 3);
    log = new ODistributedMessageLogMemory();
    promised = new OPromisedDistributedOpsImpl();
    // TODO: provide minimum quorum;
    coordinated = new OCoordinatedDistributedOpsImpl(0);
  }

  public record StartOp(OTransactionIdPromise promise, Set<ONodeId> nodes) {}
  ;

  public StartOp start(OCompleteAction action) {
    Optional<OTransactionIdPromise> prom = this.sequenceManager.next();
    Set<ONodeId> nodes = this.coordinated.start(prom.get(), action);
    return new StartOp(prom.get(), nodes);
  }

  public void success(ONodeId node, OTransactionIdPromise promise) {
    this.coordinated.success(node, promise);
  }

  public void failure(ONodeId node, OTransactionIdPromise promise) {
    this.coordinated.failure(node, promise);
  }

  public void register(ONodeId node) {
    this.coordinated.registerNode(node);
  }

  public void unregister(ONodeId node) {
    this.coordinated.unregisterNode(node);
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
