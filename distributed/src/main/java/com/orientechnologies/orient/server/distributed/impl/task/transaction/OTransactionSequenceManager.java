package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;

import java.io.*;
import java.util.*;

public class OTransactionSequenceManager {

  private volatile long[]           sequentials;
  private volatile OTransactionId[] promisedSequential;
  private final    String           node;

  public OTransactionSequenceManager(String node, int size) {
    //TODO: make size configurable
    this.sequentials = new long[size];
    this.promisedSequential = new OTransactionId[size];
    this.node = node;
  }

  public synchronized void fill(OTransactionSequenceStatus data) {
    this.sequentials = data.getStatus();
    this.promisedSequential = new OTransactionId[this.sequentials.length];
  }

  public synchronized Optional<OTransactionId> next() {
    int pos;
    int retry = 0;
    do {
      pos = new Random().nextInt(1000);
      if (retry > 1000) {
        return Optional.empty();
      }
      retry++;
    } while (this.promisedSequential[pos] != null);
    return Optional.of(nextAt(pos));
  }

  /**
   * This is publuc only for testing purposes
   *
   * @param pos
   * @return
   */
  public synchronized OTransactionId nextAt(int pos) {
    OTransactionId id = new OTransactionId(Optional.of(this.node), pos, this.sequentials[pos] + 1);
    this.promisedSequential[pos] = id;
    return id;
  }

  public synchronized List<OTransactionId> notifySuccess(OTransactionId transactionId) {
    if (this.promisedSequential[transactionId.getPosition()] != null) {
      if (this.promisedSequential[transactionId.getPosition()].getSequence() == transactionId.getSequence()) {
        this.sequentials[transactionId.getPosition()] = transactionId.getSequence();
        this.promisedSequential[transactionId.getPosition()] = null;
      } else {
        List<OTransactionId> missing = new ArrayList<>();
        for (long x = this.promisedSequential[transactionId.getPosition()].getSequence() + 1;
             x <= transactionId.getSequence(); x++) {
          missing.add(new OTransactionId(Optional.empty(), transactionId.getPosition(), x));
        }
        return missing;
      }
    } else {
      if (this.sequentials[transactionId.getPosition()] + 1 == transactionId.getSequence()) {
        // Not promised but valid, accept it
        //TODO: may need to return this information somehow
        this.sequentials[transactionId.getPosition()] = transactionId.getSequence();
      } else {
        List<OTransactionId> missing = new ArrayList<>();
        for (long x = this.sequentials[transactionId.getPosition()] + 1; x <= transactionId.getSequence(); x++) {
          missing.add(new OTransactionId(Optional.empty(), transactionId.getPosition(), x));
        }
        return missing;
      }
    }
    return Collections.emptyList();
  }

  public synchronized Optional<OTransactionId> validateTransactionId(OTransactionId transactionId) {
    if (this.promisedSequential[transactionId.getPosition()] == null
        && this.sequentials[transactionId.getPosition()] + 1 == transactionId.getSequence()) {
      this.promisedSequential[transactionId.getPosition()] = transactionId;
      return Optional.empty();
    } else {
      Optional<String> owner;
      if (this.promisedSequential[transactionId.getPosition()] != null) {
        owner = this.promisedSequential[transactionId.getPosition()].getNodeOwner();
      } else {
        owner = Optional.empty();
      }
      return Optional.of(new OTransactionId(owner, transactionId.getPosition(), this.sequentials[transactionId.getPosition()]));
    }
  }

  public synchronized List<OTransactionId> checkSelfStatus(OTransactionSequenceStatus sequenceStatus) {
    long[] status = sequenceStatus.getStatus();
    List<OTransactionId> missing = null;
    for (int i = 0; i < status.length; i++) {
      if (this.sequentials[i] < status[i]) {
        if (this.promisedSequential[i] == null) {
          if (missing == null) {
            missing = new ArrayList<>();
          }
          for (long x = this.sequentials[i] + 1; x <= status[i]; x++) {
            missing.add(new OTransactionId(Optional.empty(), i, x));
          }
        } else if (this.promisedSequential[i].getSequence() != status[i]) {
          if (missing == null) {
            missing = new ArrayList<>();
          }
          for (long x = this.promisedSequential[i].getPosition() + 1; x <= status[i]; x++) {
            missing.add(new OTransactionId(Optional.empty(), i, x));
          }
        }
      }
    }
    if (missing == null) {
      missing = Collections.emptyList();
    }
    return missing;
  }

  public synchronized List<OTransactionId> checkOtherStatus(OTransactionSequenceStatus sequenceStatus) {
    long[] status = sequenceStatus.getStatus();
    List<OTransactionId> missing = null;
    for (int i = 0; i < status.length; i++) {
      if (this.sequentials[i] > status[i]) {
        if (missing == null) {
          missing = new ArrayList<>();
        }
        for (long x = status[i] + 1; x <= this.sequentials[i]; x++) {
          missing.add(new OTransactionId(Optional.empty(), i, x));
        }
      }
    }
    if (missing == null) {
      missing = Collections.emptyList();
    }
    return missing;
  }

  public synchronized boolean notifyFailure(OTransactionId id) {
    OTransactionId promised = this.promisedSequential[id.getPosition()];
    if (promised != null) {
      if (promised.getSequence() == id.getSequence() && promised.getNodeOwner().equals(id.getNodeOwner())) {
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
