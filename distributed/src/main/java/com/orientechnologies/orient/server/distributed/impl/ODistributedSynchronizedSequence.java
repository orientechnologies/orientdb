package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.tx.*;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionSequenceManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class ODistributedSynchronizedSequence {
  private final OTransactionSequenceManager sequenceManager;
  private       CountDownLatch              request;

  public ODistributedSynchronizedSequence(String node, int size) {
    sequenceManager = new OTransactionSequenceManager(node, size);
    request = new CountDownLatch(1);
    request.countDown();
  }

  public Optional<OTransactionId> validateTransactionId(OTransactionId id) {
    return sequenceManager.validateTransactionId(id);
  }

  public void notifyFailure(OTransactionId id) {
    sequenceManager.notifyFailure(id);
  }

  public Optional<OTransactionId> next() {
    return sequenceManager.next();
  }

  public synchronized OTxMetadataHolderImpl notifySuccess(OTransactionId id) {
    try {
      request.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    List<OTransactionId> data = sequenceManager.notifySuccess(id);
    if (data.isEmpty()) {
      request = new CountDownLatch(1);
      return new OTxMetadataHolderImpl(request, id, sequenceManager.currentStatus());
    } else {
      throw new RuntimeException("");
    }
  }

  public List<OTransactionId> missingTransactions(OTransactionSequenceStatus lastState) {
    return sequenceManager.checkOtherStatus(lastState);
  }

  public void fill(Optional<byte[]> lastMetadata) {
    lastMetadata.ifPresent((data) -> sequenceManager.fill(OTxMetadataHolderImpl.read(data).getStatus()));
  }

  public OTransactionSequenceStatus currentStatus() {
    return sequenceManager.currentStatus();
  }

  public List<OTransactionId> checkSelfStatus(OTransactionSequenceStatus status) {
    return sequenceManager.checkSelfStatus(status);
  }
}
