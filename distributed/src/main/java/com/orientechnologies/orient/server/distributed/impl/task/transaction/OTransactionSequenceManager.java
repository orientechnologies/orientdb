package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.common.log.OLogManager;
import jdk.nashorn.internal.runtime.options.Option;

import java.io.*;
import java.util.*;

public class OTransactionSequenceManager {

  private volatile long[] sequentials;
  private volatile Long[] promisedSequential;

  public OTransactionSequenceManager() {
    //TODO: make configurable
    this.sequentials = new long[1000];
    this.promisedSequential = new Long[1000];
  }

  public void fill(byte[] data) {
    DataInput dataInput = new DataInputStream(new ByteArrayInputStream(data));
    int len = 0;
    try {
      len = dataInput.readInt();

      long[] newSequential = new long[len];
      for (int i = 0; i < len; i++) {
        newSequential[i] = dataInput.readLong();
      }
      this.sequentials = newSequential;
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error in deserialization", e);
    }
  }

  public synchronized byte[] store() {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(buffer);
    try {
      dataOutput.writeInt(this.sequentials.length);
      for (int i = 0; i < this.sequentials.length; i++) {
        dataOutput.writeLong(this.sequentials[i]);
      }
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error in serialization", e);
    }
    return buffer.toByteArray();
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
    this.promisedSequential[pos] = this.sequentials[pos] + 1;
    long sequence = this.promisedSequential[pos];
    return new OTransactionId(pos, sequence);
  }

  public synchronized List<OTransactionId> notifySuccess(OTransactionId transactionId) {
    if (this.promisedSequential[transactionId.getPosition()] != null) {
      if (this.promisedSequential[transactionId.getPosition()] == transactionId.getSequence()) {
        this.sequentials[transactionId.getPosition()] = transactionId.getSequence();
        this.promisedSequential[transactionId.getPosition()] = null;
      } else {
        List<OTransactionId> missing = new ArrayList<>();
        for (long x = this.promisedSequential[transactionId.getPosition()].longValue() + 1; x <= transactionId.getSequence(); x++) {
          missing.add(new OTransactionId(transactionId.getPosition(), x));
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
          missing.add(new OTransactionId(transactionId.getPosition(), x));
        }
        return missing;
      }
    }
    return Collections.emptyList();
  }

  public synchronized boolean validateTransactionId(OTransactionId transactionId) {
    if (this.promisedSequential[transactionId.getPosition()] == null) {
      this.promisedSequential[transactionId.getPosition()] = transactionId.getSequence();
      return true;
    } else {
      return false;
    }
  }

  public synchronized List<OTransactionId> checkStatus(long[] status) {
    List<OTransactionId> missing = null;
    for (int i = 0; i < status.length; i++) {
      if (this.sequentials[i] < status[i]) {
        if (this.promisedSequential[i] == null) {
          if (missing == null) {
            missing = new ArrayList<>();
          }
          for (long x = this.sequentials[i] + 1; x <= status[i]; x++) {
            missing.add(new OTransactionId(i, x));
          }
        } else if (this.promisedSequential[i].longValue() != status[i]) {
          if (missing == null) {
            missing = new ArrayList<>();
          }
          for (long x = this.promisedSequential[i].longValue() + 1; x <= status[i]; x++) {
            missing.add(new OTransactionId(i, x));
          }
        }
      }
    }
    if (missing == null) {
      missing = Collections.emptyList();
    }
    return missing;
  }

  public synchronized long[] currentStatus() {
    return Arrays.copyOf(this.sequentials, this.sequentials.length);
  }

}
