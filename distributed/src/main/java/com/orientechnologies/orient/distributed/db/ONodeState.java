package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.tx.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionIdPromise;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderImpl;
import com.orientechnologies.orient.core.tx.ValidationResult;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.exception.OTransactionAlreadyPresentException;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionSequenceManager;
import java.util.List;
import java.util.Optional;

public class ONodeState {

  private OTransactionSequenceManager sequenceManager;

  public ONodeState(ONodeId coordinator) {
    sequenceManager = new OTransactionSequenceManager(coordinator, 3);
  }

  public ValidationResult validateTransactionId(OTransactionIdPromise id) {
    return sequenceManager.validateTransactionId(id);
  }

  public void notifyFailure(OTransactionIdPromise id) {
    sequenceManager.notifyFailure(id);
  }

  public Optional<OTransactionIdPromise> next() {
    return sequenceManager.next();
  }

  public Optional<ORawPair<OTransactionIdPromise, OTransactionIdPromise>> nextDDL() {
    return sequenceManager.nextDDL();
  }

  public synchronized OTxMetadataHolderImpl notifySuccess(OTransactionIdPromise id) {
    ValidationResult status = sequenceManager.notifySuccess(id);
    if (status == ValidationResult.ALREADY_PRESENT) {
      throw new OTransactionAlreadyPresentException("Tx Already present in the current context");
    } else {
      throw new ODistributedException("Failed transaction sequence need a reinstall");
    }
  }

  public List<OTransactionId> missingTransactions(OTransactionSequenceStatus lastState) {
    return sequenceManager.checkOtherStatus(lastState);
  }

  public void fill(Optional<byte[]> lastMetadata) {
    lastMetadata.ifPresent(
        (data) -> sequenceManager.fill(OTxMetadataHolderImpl.read(data).getStatus()));
  }

  public OTransactionSequenceStatus currentStatus() {
    return sequenceManager.currentStatus();
  }

  public List<OTransactionId> checkSelfStatus(OTransactionSequenceStatus status) {
    return sequenceManager.checkSelfStatus(status);
  }
}
