package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class OTransactionSequenceManager {

  private UUID                      selfIdentifier;
  private long                      next;
  private Optional<Long>            lastSuccess;
  private Optional<UUID>            lastSuccessSender;
  private Map<Long, OTransactionId> promised;

  public synchronized OTransactionId next() {
    long next = this.next;
    this.next += 1;
    OTransactionId id = new OTransactionId(this.selfIdentifier, next, this.lastSuccess, this.lastSuccessSender);
    this.promised.put(next, id);
    return id;
  }

  public synchronized void notifySuccess(long lastSuccess, UUID lastSuccessSender) {
    // TODO: check previous success order
    this.lastSuccess = Optional.of(lastSuccess);
    this.lastSuccessSender = Optional.of(lastSuccessSender);
  }

  public boolean validateTransactionId(OTransactionId transactionId) {
    if (transactionId.getSequence() > next) {
      if (this.lastSuccess.isPresent()) {
        if (lastSuccess.equals(transactionId.getLastSuccessful()) && lastSuccessSender.equals(transactionId.getLastRequester())) {
          this.promised.put(transactionId.getSequence(), transactionId);
          return true;
        }
      } else {
        this.promised.put(transactionId.getSequence(), transactionId);
        return true;
      }
    }
    return false;
  }

}
