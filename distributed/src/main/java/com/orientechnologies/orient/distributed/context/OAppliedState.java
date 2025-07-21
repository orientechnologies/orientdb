package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionId;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/** This represent the applied state, in a normal flow this would be part of the storage
 * and this may lag behind the log because it depends on the time of the executions of apply.
 *
 */
public class OAppliedState {

  private volatile long[] sequentials;
  private final OWatchPromise[] watches;

  public OAppliedState(int size) {
    this.sequentials = new long[size];
    this.watches = new OWatchPromise[size];
  }

  public synchronized Optional<CountDownLatch> watch(OTransactionId id) {
    if (this.sequentials[id.getPosition()] >= id.getSequence()) {
      // Already applied, nothing to wait
      return Optional.empty();
    }
    OWatchPromise watch = this.watches[id.getPosition()];
    if (watch == null) {
      watch = new OWatchPromise();
      this.watches[id.getPosition()] = watch;
    }
    return Optional.of(watch.watch(id.getSequence()));
  }

  public synchronized void complete(OTransactionId id) {
    if (this.sequentials[id.getPosition()] >= id.getSequence()) {
      // No Op, already applied
      return;
    }
    assert this.sequentials[id.getPosition()] == id.getSequence() - 1;
    this.sequentials[id.getPosition()] = id.getSequence();
    OWatchPromise watch = this.watches[id.getPosition()];
    if (watch != null) {
      watch.complete(id.getSequence());
    }
  }
}
