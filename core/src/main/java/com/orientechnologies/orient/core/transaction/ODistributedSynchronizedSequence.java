package com.orientechnologies.orient.core.transaction;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OTransactionAlreadyPresentException;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolder;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderSyncOrder;
import com.orientechnologies.orient.core.tx.SuccessResult;
import com.orientechnologies.orient.core.tx.ValidationResult;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class ODistributedSynchronizedSequence {
  private OLogger logger = OLogManager.instance().logger(ODistributedSynchronizedSequence.class);
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
  public synchronized OTxMetadataHolder notifySuccess(OTransactionIdPromise id) {
    try {
      request.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    SuccessResult status = sequenceManager.notifySuccess(id);
    if (status == SuccessResult.ALREADY_PRESENT) {
      throw new OTransactionAlreadyPresentException("Tx Already present in the current context");
    } else if (status == SuccessResult.VALID || status == SuccessResult.VALID_MISSING) {
      request = new CountDownLatch(1);
      return new OTxMetadataHolderSyncOrder(request, id.getId(), sequenceManager.currentStatus());
    } else {
      throw new ODatabaseException("Failed transaction sequence need a reinstall");
    }
  }

  public List<OTransactionId> missingTransactions(OTransactionSequenceStatus lastState) {
    return sequenceManager.checkOtherStatus(lastState);
  }

  public boolean missingDDL(OTransactionSequenceStatus lastState) {
    return sequenceManager.checkOtherStatusDDL(lastState);
  }

  public void fill(Optional<byte[]> lastMetadata) {
    logger.debug("fill called has metadata: %s", lastMetadata.isPresent());
    lastMetadata.ifPresent(
        (data) -> sequenceManager.fill(OTxMetadataHolder.read(data).getStatus()));
  }

  public OTransactionSequenceStatus currentStatus() {
    return sequenceManager.currentStatus();
  }

  public List<OTransactionId> checkSelfStatus(OTransactionSequenceStatus status) {
    return sequenceManager.checkSelfStatus(status);
  }

  public long debugStatus(int position) {
    return sequenceManager.debugGetSequence(position);
  }

  public OTxMetadataHolder localSuccess() {
    var next = sequenceManager.next();
    // TODO: make it not looping blocking forever
    while (next.isEmpty()) next = sequenceManager.next();
    sequenceManager.notifySuccess(next.get());
    return new OTxMetadataHolderLocal(next.get().getId(), sequenceManager.currentStatus());
  }
}
