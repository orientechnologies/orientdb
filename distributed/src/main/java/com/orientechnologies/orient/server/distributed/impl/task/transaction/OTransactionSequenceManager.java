package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.ValidationResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class OTransactionSequenceManager {

  private volatile long[] sequentials;
  private volatile OTransactionId[] promisedSequential;
  private final String node;
  private final int sequenceSize;

  public OTransactionSequenceManager(String node, int size) {
    // TODO: make size configurable
    this.sequentials = new long[size];
    this.promisedSequential = new OTransactionId[size];
    this.node = node;
    this.sequenceSize = size;
  }

  public synchronized void fill(OTransactionSequenceStatus data) {
    this.sequentials = data.getStatus();
    this.promisedSequential = new OTransactionId[this.sequentials.length];
  }

  public synchronized Optional<OTransactionId> next() {
    int pos;
    int retry = 0;
    do {
      pos = new Random().nextInt(sequenceSize);
      if (retry > sequenceSize) {
        return Optional.empty();
      }
      retry++;
    } while (this.promisedSequential[pos] != null);
    return Optional.of(nextAt(pos));
  }

  /**
   * This is public only for testing purposes
   *
   * @param pos
   * @return
   */
  public synchronized OTransactionId nextAt(int pos) {
    OTransactionId id = new OTransactionId(Optional.of(this.node), pos, this.sequentials[pos] + 1);
    this.promisedSequential[pos] = id;
    return id;
  }

  public synchronized ValidationResult notifySuccess(OTransactionId transactionId) {
    if (this.promisedSequential[transactionId.getPosition()] != null) {
      if (this.promisedSequential[transactionId.getPosition()].getSequence()
          == transactionId.getSequence()) {
        this.sequentials[transactionId.getPosition()] = transactionId.getSequence();
        this.promisedSequential[transactionId.getPosition()] = null;
      } else if (this.promisedSequential[transactionId.getPosition()].getSequence()
          > transactionId.getSequence()) {
        return ValidationResult.ALREADY_PRESENT;
      } else {
        return ValidationResult.MISSING_PREVIOUS;
      }
    } else {
      if (this.sequentials[transactionId.getPosition()] + 1 == transactionId.getSequence()) {
        // Not promised but valid, accept it
        // TODO: may need to return this information somehow
        this.sequentials[transactionId.getPosition()] = transactionId.getSequence();
      } else if (this.sequentials[transactionId.getPosition()] + 1 > transactionId.getSequence()) {
        return ValidationResult.ALREADY_PRESENT;
      } else {
        return ValidationResult.MISSING_PREVIOUS;
      }
    }
    return ValidationResult.VALID;
  }

  public synchronized ValidationResult validateTransactionId(OTransactionId transactionId) {
    if (this.promisedSequential[transactionId.getPosition()] == null) {
      if (this.sequentials[transactionId.getPosition()] + 1 == transactionId.getSequence()) {
        this.promisedSequential[transactionId.getPosition()] = transactionId;
        return ValidationResult.VALID;
      } else if (this.sequentials[transactionId.getPosition()] + 1 < transactionId.getSequence()) {
        return ValidationResult.MISSING_PREVIOUS;
      } else {
        return ValidationResult.ALREADY_PRESENT;
      }
    } else {
      if (this.sequentials[transactionId.getPosition()] + 1 == transactionId.getSequence()) {
        if (this.promisedSequential[transactionId.getPosition()]
            .getNodeOwner()
            .equals(transactionId.getNodeOwner())) {
          return ValidationResult.VALID;
        } else {
          return ValidationResult.ALREADY_PROMISED;
        }
      } else if (this.sequentials[transactionId.getPosition()] + 1 < transactionId.getSequence()) {
        return ValidationResult.MISSING_PREVIOUS;
      } else {
        return ValidationResult.ALREADY_PRESENT;
      }
    }
  }

  public synchronized List<OTransactionId> checkSelfStatus(
      OTransactionSequenceStatus sequenceStatus) {
    long[] status = sequenceStatus.getStatus();
    List<OTransactionId> missing = new ArrayList<>();
    for (int i = 0; i < status.length; i++) {
      if (this.sequentials[i] < status[i]) {
        if (this.promisedSequential[i] == null) {
          for (long x = this.sequentials[i] + 1; x <= status[i]; x++) {
            missing.add(new OTransactionId(Optional.empty(), i, x));
          }
        } else if (this.promisedSequential[i].getSequence() != status[i]) {
          for (long x = this.promisedSequential[i].getPosition() + 1; x <= status[i]; x++) {
            missing.add(new OTransactionId(Optional.empty(), i, x));
          }
        }
      }
    }
    return missing;
  }

  public synchronized List<OTransactionId> checkOtherStatus(
      OTransactionSequenceStatus sequenceStatus) {
    long[] status = sequenceStatus.getStatus();
    List<OTransactionId> missing = new ArrayList<>();
    for (int i = 0; i < status.length; i++) {
      if (this.sequentials[i] > status[i]) {
        for (long x = status[i] + 1; x <= this.sequentials[i]; x++) {
          missing.add(new OTransactionId(Optional.empty(), i, x));
        }
      }
    }
    return missing;
  }

  public synchronized boolean notifyFailure(OTransactionId id) {
    OTransactionId promised = this.promisedSequential[id.getPosition()];
    if (promised != null) {
      if (promised.getSequence() == id.getSequence()
          && promised.getNodeOwner().equals(id.getNodeOwner())) {
        this.promisedSequential[id.getPosition()] = null;
        return true;
      }
    }
    return false;
  }

  public synchronized OTransactionSequenceStatus currentStatus() {
    return new OTransactionSequenceStatus(Arrays.copyOf(this.sequentials, this.sequentials.length));
  }
}
