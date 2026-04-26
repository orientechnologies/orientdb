package com.orientechnologies.orient.core.transaction;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.ValidationResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class OTransactionSequenceManager {

  private volatile long[] sequentials;
  private volatile OTransactionIdPromise[] promisedSequential;
  private final ONodeId coordinator;
  private final int sequenceSize;

  public OTransactionSequenceManager(ONodeId coordinator, int size) {
    if (size < 2) {
      throw new OConfigurationException("Sequence size need to be at least of size 3");
    }
    this.sequentials = new long[size];
    this.promisedSequential = new OTransactionIdPromise[size];
    this.coordinator = coordinator;
    // Reserve position 0,1 for DDLs, so the random range is one less
    this.sequenceSize = size - 2;
  }

  public static byte[] initData(int size) throws IOException {
    return new OTransactionSequenceStatus(new long[size]).store();
  }

  public synchronized void fill(OTransactionSequenceStatus data) {
    this.sequentials = data.getStatus();
    this.promisedSequential = new OTransactionIdPromise[this.sequentials.length];
  }

  public synchronized Optional<OTransactionIdPromise> next() {
    int pos;
    int retry = 0;
    do {
      // Position 0,2 are for DDLs so add 2 to generate number
      pos = new Random().nextInt(sequenceSize) + 2;
      if (retry > sequenceSize) {
        return Optional.empty();
      }
      retry++;
    } while (this.promisedSequential[pos] != null);
    return Optional.of(nextAt(pos));
  }

  /** As today DDLs are not atomic, so we used two sequential for pre-operation and
   * post operation to assert that the DDL was completed, as soon as DDLs will be
   * atomic we can revert to a single sequential
   * @return
   */
  public synchronized Optional<ORawPair<OTransactionIdPromise, OTransactionIdPromise>> nextDDL() {
    if (this.promisedSequential[0] != null || this.promisedSequential[1] != null) {
      return Optional.empty();
    }
    return Optional.of(new ORawPair<>(nextAt(0), nextAt(1)));
  }

  /**
   * This is public only for testing purposes
   *
   * @param pos
   * @return
   */
  public synchronized OTransactionIdPromise nextAt(int pos) {
    OTransactionId id = new OTransactionId(pos, this.sequentials[pos] + 1);
    OTransactionIdPromise promise = new OTransactionIdPromise(this.coordinator, id);
    this.promisedSequential[pos] = promise;
    return promise;
  }

  public synchronized ValidationResult notifySuccess(OTransactionIdPromise promise) {
    OTransactionId transactionId = promise.getId();
    OTransactionIdPromise promised = this.promisedSequential[transactionId.getPosition()];
    if (promised != null) {
      if (promised.getId().getSequence() == transactionId.getSequence()) {
        this.sequentials[transactionId.getPosition()] = transactionId.getSequence();
        this.promisedSequential[transactionId.getPosition()] = null;
      } else {
        if (promised.getId().getSequence() > transactionId.getSequence()) {
          return ValidationResult.ALREADY_PRESENT;
        } else {
          return ValidationResult.MISSING_PREVIOUS;
        }
      }
    } else {
      long nextSequantial = this.sequentials[transactionId.getPosition()] + 1;
      if (nextSequantial == transactionId.getSequence()) {
        // Not promised but valid, accept it
        // TODO: may need to return this information somehow
        this.sequentials[transactionId.getPosition()] = transactionId.getSequence();
      } else if (nextSequantial > transactionId.getSequence()) {
        return ValidationResult.ALREADY_PRESENT;
      } else {
        return ValidationResult.MISSING_PREVIOUS;
      }
    }
    return ValidationResult.VALID;
  }

  public synchronized ValidationResult validate(OTransactionIdPromise promise) {
    OTransactionId transactionId = promise.getId();
    OTransactionIdPromise promised = this.promisedSequential[transactionId.getPosition()];
    long nextSequential = this.sequentials[transactionId.getPosition()] + 1;
    if (promised == null) {
      if (nextSequential == transactionId.getSequence()) {
        this.promisedSequential[transactionId.getPosition()] = promise;
        return ValidationResult.VALID;
      } else if (nextSequential < transactionId.getSequence()) {
        return ValidationResult.MISSING_PREVIOUS;
      } else {
        return ValidationResult.ALREADY_PRESENT;
      }
    } else {
      if (nextSequential == transactionId.getSequence()) {
        if (promised.getCoordinator().equals(promise.getCoordinator())) {
          return ValidationResult.VALID;
        } else if (promised.nextAccept(promise)) {
          this.promisedSequential[transactionId.getPosition()] = promise;
          return ValidationResult.VALID;
        } else {
          return ValidationResult.ALREADY_PROMISED;
        }
      } else if (nextSequential < transactionId.getSequence()) {
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
            missing.add(new OTransactionId(i, x));
          }
        } else if (this.promisedSequential[i].getId().getSequence() != status[i]) {
          for (long x = this.promisedSequential[i].getId().getPosition() + 1; x <= status[i]; x++) {
            missing.add(new OTransactionId(i, x));
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
          missing.add(new OTransactionId(i, x));
        }
      }
    }
    return missing;
  }

  public synchronized boolean notifyFailure(OTransactionIdPromise promise) {
    OTransactionId id = promise.getId();
    OTransactionIdPromise promised = this.promisedSequential[id.getPosition()];
    if (promised != null) {
      if (promised.getId().getSequence() == id.getSequence()
          && promised.getCoordinator().equals(promise.getCoordinator())) {
        this.promisedSequential[id.getPosition()] = null;
        return true;
      }
    }
    return false;
  }

  public synchronized OTransactionSequenceStatus currentStatus() {
    return new OTransactionSequenceStatus(Arrays.copyOf(this.sequentials, this.sequentials.length));
  }

  public synchronized long debugGetSequence(int position) {
    return this.sequentials[position];
  }

  public synchronized boolean isApplied(OTransactionId txId) {
    return this.sequentials[txId.getPosition()] >= txId.getSequence();
  }

  public OTransactionIdPromise promised(int position) {
    return this.promisedSequential[position];
  }
}
