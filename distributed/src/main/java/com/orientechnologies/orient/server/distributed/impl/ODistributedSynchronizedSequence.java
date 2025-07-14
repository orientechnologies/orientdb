package com.orientechnologies.orient.server.distributed.impl;

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
import java.util.concurrent.CountDownLatch;

public class ODistributedSynchronizedSequence {
  private final OTransactionSequenceManager sequenceManager;
  private volatile CountDownLatch request;

  public ODistributedSynchronizedSequence(ONodeId node, int size) {
    sequenceManager = new OTransactionSequenceManager(node, size);
    request = new CountDownLatch(1);
    request.countDown();
  }

  public ValidationResult validate(OTransactionIdPromise id) {
    return sequenceManager.validate(id);
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

  /** This make sure that there is a synchronization between the apply of the status of transactions sequence
   * and the logging of it in the database, to avoid to have in the status a transaction that has not yet been logged.
   *
   * @param id
   * @return
   */
  public synchronized OTxMetadataHolderImpl notifySuccess(OTransactionIdPromise id) {
    try {
      request.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    ValidationResult status = sequenceManager.notifySuccess(id);
    if (status == ValidationResult.ALREADY_PRESENT) {
      throw new OTransactionAlreadyPresentException("Tx Already present in the current context");
    } else if (status == ValidationResult.VALID) {
      request = new CountDownLatch(1);
      return new OTxMetadataHolderImpl(request, id.getId(), sequenceManager.currentStatus());
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
